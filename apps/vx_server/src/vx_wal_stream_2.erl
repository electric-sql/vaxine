-module(vx_wal_stream_2).
-behaviour(gen_statem).

-export([ start_link/1,
          start_replication/4,
          stop_replication/1,
          notify_commit/5
        ]).

-export([ init/1,
          callback_mode/0,
          terminate/3
        ]).

-export([ init_stream/3,
          await_client/3,
          await_wal/3,
          await_materializer/3
        ]).

-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("antidote/include/antidote.hrl").
-include_lib("antidote/include/meta_data_notif_server.hrl").
-include("vx_wal_stream.hrl").
-include("vx_wal.hrl").

%% In milliseconds
-define(POLL_RETRY_MIN, 100).
-define(POLL_RETRY_MAX, timer:seconds(10)).

-record(wal_trans,
        { txid :: antidote:txid(),
          %% dcid is not determined for aborted transactions
          dcid :: antidote:dcid() | undefined,
          %% Wal offset, that points to first transaction record that
          %% contains update_start.
          start_offset :: vx_wal_utils:file_offset(),
          %% Wal offset, that points to position before commit record
          end_offset :: vx_wal_utils:file_offset(),
          %% Snapshot here is snapshot of transaction updated with
          %% commit timestamp of the transaction
          snapshot = none :: antidote:snapshot_time() | none,
          %% Materialized is left with none for aborted transactions
          materialized = none :: list() | none
        }).

-record(txn_iter, { last_commit_offset :: vx_wal_utils:file_offset(),
                    last_tx_id :: antidote:txid() | undefined,
                    not_comitted_offsets = [] ::
                      orddict:orddict(non_neg_integer(), txid()),
                    not_comitted_txns = #{} :: txns_noncommitted_map(),
                    comitted_txns = []
                  }).

-record(data, {
               client_pid :: pid() | undefined,
               client_mon_ref :: reference() | undefined,

               txn_iter :: txn_iter(),

               partition :: antidote:partition_id(),
               port :: port() | undefined,
               port_retry_tref :: reference() | undefined,
               port_retry_backoff :: backoff:backoff(),

               send_raw_values = false :: boolean(),

               wal_info :: #wal{} | undefined
              }).

-type txns_noncomitted_map() :: #{antidote:txid() =>
                                      { vx_wals_utils:file_offset(),
                                        [any_log_payload()]
                                      }
                                 }.
-type txns_comitted() :: [ { antidote:txid(),
                             antidote:dcid(),
                             antidote:clock_time(),
                             vx_wals_utils:file_offset(),
                             term() }
                         ].

callback_mode() ->
    state_functions.

init(_Args) ->
    %% FIXME: Only work with a single partition at the moment
    %% FIXME: Move this initialization back to start_replication
    erlang:process_flag(trap_exit, true),
    [Partition] = dc_utilities:get_all_partitions(),

    PortSendBackoff0 = backoff:init(?POLL_RETRY_MIN, ?POLL_RETRY_MAX,
                                    self(), port_send_retry),
    PortSendBackoff1 = backoff:type(PortSendBackoff0, jitter),

    ok = vx_wal_stream_server:register(),
    {ok, init_stream, #data{partition = Partition,
                            port_retry_backoff = PortSendBackoff1
                           }}.

init_stream({call, {Sender, _} = F}, {start_replication, ReqRef, Port, Opts}, Data) ->
    %% FIXME: We support only single partition for now
    LogFile = vx_wal_utils:get_partition_wal_path(Data#data.partition),
    Lsn = {_LWalOffset0, RWalOffset0} = vx_wal_utils:get_lsn(
                                          proplists:get_value(offset, Opts, 0)
                                         ),
    {ok, WalState} = vx_wal_utils:open_wal(LogFile, Lsn),

    MonRef = erlang:monitor(process, Sender),

    true = init_notification_slot(Data#data.partition),
    ok = logging_notification_server:add_handler(
           ?MODULE, notify_commit, [self()]),

    ReplicationRef = make_ref(),
    ok = vx_wal_tcp_worker:send_start(Port, ReqRef, ReplicationRef),

    {next_state, stream_catchup,
     Data#data{
       client_pid = Sender,
       client_mon_ref = MonRef,
       wal_info = WalState,
       txn_iter = #txn_iter{ last_commit_offset = RWalOffset0 },
       send_raw_values = proplists:get_bool(raw_values, Opts),
       port = Port
      },
     [{next_event, internal, continue_stream},
      {reply, F, {ok, ReplicationRef}}
     ]};

init_stream(Type, Msg, Data) ->
    common_callback(Type, Msg, Data).

stream_catchup(internal, continue_stream, Data) ->
    continue_wal_reading(Data).

common_callback(info, {gen_event_EXIT, _Handler, _Reason}, Data) ->
    {keep_state, Data};

common_callback(info, {inet_reply, _Sock, ok}, _Data) ->
    keep_state_and_data;

common_callback(info, {inet_reply, _Sock, {error, Reason}}, Data) ->
    logger:error("socket error: ~p~n", [Reason]),
    {stop, {shutdown, Reason}, Data}.

terminate(_Reason, _SN, #data{} = Data) ->
    _ = maybe_cancel_timer(Data#data.port_retry_tref),
    _ = maybe_close_file(Data#data.wal_info),
    ok.


%%------------------------------------------------------------------------------

consume_ops_from_wal(#data{wal_info = Wal} = Data) ->
    logger:debug("Continue wal streaming for client ~p on partition ~p"
                " at position ~p~n",
                [Data#data.client_pid, Data#data.partition, Wal#wal.file_pos]),
    consume_ops_from_wal(Data#data.txn_iter, Data#data.wal_info, Data).

consume_ops_from_wal(TxnIter, WalInfo, Data) ->
    case vx_wal_utils:read_ops_from_log(fun process_ops/3, TxnIter, WalInfo) of
        {ok, Acc1, Wal1} -> %% We reached end of WAL log
            ok;
        {stop, Acc1, Wal1} -> %% There is data to send
            case notify_client(Port, Acc1#txn_iter.comitted_txns,
                               Data#data.send_raw_values)
            of
                {continue, _} ->
                    consume_ops_from_wal(   );
                {client_stop, LastProcessedTxn, ComittedTxns, NotComittedBorders} ->
                    %% Poll the client, or wait for async confirmation
                    ok;
                {cache_stop, LastProcessedTxn, ComittedTxns, NotComittedBorders} ->
                    %% We can not materialize the data
                    ok
            end;
        {error, _} = Error ->
           {stop, Error}
    end.

process_ops(NewOps, LogOffset, #txn_iter{} = Acc) ->
    #txn_iter{last_tx_id = LastTxId,
              not_comitted_offsets = NotComittedBorders,
              not_comitted_txns = NotComitted
             } = Acc,
    {ok, ComittedData, {LastTxId1, NotComitted1, NotComittedBorders1}} =
        process_txns(NewOps, NotComitted, [], LogOffset,
                     LastTxId, NotComittedBorders),
    case ComittedData of
        [] ->
            {ok, Acc#txn_iter{last_tx_id = LastTxId1,
                              not_comitted_txns = NotComitted1,
                              not_comitted_offsets = NotComittedBorders1
                             }};

        _ ->
            {stop, Acc#txn_iter{last_tx_id = LastTxId1,
                                not_comitted_txns = NotComitted1,
                                not_comitted_offsets = NotComittedBorders1,
                                comitted_txns = ComittedData
                               }}
    end.

filter_fun(#wal_trans{materialized = none}, #txn_iter{}) ->
    true;
filter_fun(#wal_trans{end_offset = EndOffset},
           #txn_iter{last_commit_offset = RightOffsetBorder})
  when EndOffset =< RightOffsetBorder ->
    true;
filter_fun(#wal_trans{}, _) ->
    false.


notify_client(Comitted) ->
    try notify_client0(Port, Comitted)
    of
        {ok} ->
            ok
    end.

% FIXME: RIGHT_OFFSET THAT WE USE OT IGNORE TRANSACTIONS SHOULD BE SET WHEN START REPLICATION

notify_client0([#wal_trans{materialized = none} | FinalyzedTxns],
               LastSentTx, Port, LB, RB, RawValuesB) ->
    notify_client0(FinalyzedTxns, LastSentTx, Port, LB, RB, RawValuesB);
notify_client0([#wal_trans{txid = TxId, end_offset = EOffset} | FinalyzedTxns],
               LastSentTx, Port, LB, RB, RawValuesB)
  when EOffset =< RB ->
    logger:info("skip transaction with commit offset: ~p~n", [EOffset]),
    notify_client0(FinalyzedTxns, LastSentTx, Port,
                   remove_left_border(TxId, LB), RB, RawValuesB);
notify_client0([#wal_trans{} = Tx | FinalizedTxns] = Txns, LastTxn,
               Port, LB, RightOffset, RawValuesB) ->
    %% FIXME: We have a general assumption here, that transactions in the WAL
    %% log are sorted according to snapshot order, which is not the case in current
    %% wal implementation. This should not cause issues in single partition case
    %% though.
    case is_materialization_ready(Tx) of
        true ->
            case notify_client1(Tx, Port, LB, RawValuesB) of
                ok ->
                    notify_client0(FinalizedTxns, Tx#wal_trans.txid,
                                   Port, remove_left_border(Tx#wal_trans.txid, LB),
                                   RightOffset, RawValuesB);
                {error, _} = Error ->
                    Error;
                false ->
                    {stop, Txns, LastTxn, LB}
            end;
        false ->
            {stop, Txns, LastTxn, LB}
    end;
notify_client0([], LastTxn, _, LB, RB, _) ->
    {ok, LastTxn, LB, RB}.

notify_client1(#wal_trans{txid = TxId, dcid = DcId, snapshot = TxST,
                          materialized = TxOpsList,
                          start_offset = SOffset,
                          end_offset = EOffset
                         }, Port, LB, RawValuesB) ->
    LeftOffset = case LB of
                     [] -> SOffset;
                     [{LeftOff, _} | _] -> LeftOff
                 end,
    TxOffset = {LeftOffset, EOffset},
    logger:debug("offset: ~p~n", [TxOffset]),
    TxOpsMaterialized = materialize_keys(RawValuesB, TxId, TxST, TxOpsList),
    %% Offset that is included with each transaction is calculated according to the following
    %% principal:
    %% - left border points to the position of update_start operation for oldest transasction
    %%   in the log, that has not been send to the client (including this exact trans)
    %% - right border points to the position of commit operation, for most recent transaction
    %%   that has not been send to the client (including this exact trans)
    case vx_wal_tcp_worker:send(Port, DcId, TxId, TxOffset, TxOpsMaterialized) of
        false ->
            %% We need to retry later, port is busy
            false;
        true ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

is_materialization_ready(_Txn = #wal_trans{snapshot = TxST}) ->
    try vectorclock:get(SN, TxST) of
        true ->
            true;
        false ->
            false
    catch _:_ ->
            false
    end.

materialize_keys(_, _, _, []) -> [];
materialize_keys(_RawValuesB = true, _, _, List) -> List;
materialize_keys(_RawValuesB = false, TxId, TxST, TxOpsList0) ->
    TxOpsDict = lists:foldl(fun({Key, Type, Op}, Acc) ->
                                    dict:append({Key, Type}, Op, Acc)
                            end, dict:new(), TxOpsList0),
    TxKeys = sets:to_list(
               sets:from_list(
                 lists:map(fun({Key, Type, _Op}) -> {Key, Type} end, TxOpsList0))),
    lists:map(fun({Key, Type}) ->
                      {Key, Type, materialize(Key, Type, TxST, TxId),
                       dict:fetch({Key, Type}, TxOpsDict)
                      }
              end, TxKeys).

materialize(Key, Type, ST, TxId) ->
    {Partition, _} = log_utilities:get_key_partition(Key),
    %% FIXME: Yeah, we do not expect this to fail for now. Technically
    %% we might get here when transaction has been persisted, but
    %% materializer have not bee updated.
    %% First we need to detect it somehow with materializer_vnode API.
    %% Second if materialization is not possible we need to wait for
    %% snapshot to stabilize.
    {ok, Snapshot} =
        materializer_vnode:read(
          Key, Type, ST, TxId, _PropertyList = [], Partition),
    Snapshot.














%% -spec process_txns([{term(), #log_record{}}], txns_noncomitted_map(), txns_comitted(),
%%                    file_offset(), state()) ->
%%           {ok, txns_comitted(), state()}.
%% NOTE: We do not expect transaction to have same TxOffset for update_start message and for
%% commit.
process_txns([], NotComitted, FinalizedTxns, _, LastTxId, NotComittedBorders) ->
    {Txns, LastTxId1} = preprocess_comitted(FinalizedTxns, LastTxId),
    {ok, Txns, {LastTxId1, NotComitted, NotComittedBorders}};

process_txns([{_, LogRecord} | Rest], NotComitted, FinalizedTxns0, TxOffset,
             LastTxId, NotComittedBorders) ->
    #log_record{log_operation = LogOperation} =
        log_utilities:check_log_record_version(LogRecord),

    {NotComitted1, FinalizedTxns1, NotComittedBorders1} =
        process_op0(LogOperation, NotComitted, FinalizedTxns0, TxOffset, NotComittedBorders),
    process_txns(Rest, NotComitted1, FinalizedTxns1, TxOffset, LastTxId, NotComittedBorders1).

process_op0(#log_operation{op_type = OpType, tx_id = TxId, log_payload = Payload},
           RemainingOps, FinalizedTxns, TxOffset, NotComittedBorders)
  when OpType == update_start ->
    {Key, Type, Op} = { Payload#update_log_payload.key,
                        Payload#update_log_payload.type,
                        Payload#update_log_payload.op
                      },
    RemainingOps1 = RemainingOps#{ TxId => {TxOffset, [{Key, Type, Op}]} },
    %% Record the starting offset of the new transaction
    NotComittedBorders1 = add_left_border(TxOffset, TxId, NotComittedBorders),
    {RemainingOps1, FinalizedTxns, NotComittedBorders1};

process_op0(LogOp = #log_operation{tx_id = TxId}, RemainingOps, FinalizedTxns, TxOffset,
           NotComittedBorders) ->
    case RemainingOps of
        #{ TxId := _ } ->
            {RemainingOps1, FinalizedTxns1} =
                process_op1(LogOp, RemainingOps, FinalizedTxns, TxOffset),
            {RemainingOps1, FinalizedTxns1, NotComittedBorders};
        _ ->
            {RemainingOps, FinalizedTxns, NotComittedBorders}
    end.

process_op1(#log_operation{op_type = OpType, tx_id = TxId, log_payload = Payload},
           RemainingOps, FinalizedTxns, _TxOffset)
  when OpType == update ->
    {Key, Type, Op} = { Payload#update_log_payload.key,
                        Payload#update_log_payload.type,
                        Payload#update_log_payload.op
                      },
    RemainingOps1 =
                maps:update_with(TxId, fun({Offset, Ops}) ->
                                               {Offset, [{Key, Type, Op} | Ops]}
                                       end, RemainingOps),
    {RemainingOps1, FinalizedTxns};
process_op1(#log_operation{op_type = prepare}, RemainingOps, FinalizedTxns, _TxPrepareOffset) ->
    {RemainingOps, FinalizedTxns};
process_op1(#log_operation{op_type = abort, tx_id = TxId}, RemainingOps, FinalizedTxns, TxAbortOffset) ->
    case maps:take(TxId, RemainingOps) of
        {{TxStartOffset, _}, RemainingOps1} ->
            { RemainingOps1,
              [prepare_txn_aborted(TxId, TxStartOffset, TxAbortOffset) | FinalizedTxns]
            };
        error ->
            {RemainingOps, FinalizedTxns}
    end;
process_op1(#log_operation{op_type = commit, tx_id = TxId, log_payload = Payload},
           RemainingOps, FinalizedTxns, TxCommitOffset) ->
    #commit_log_payload{commit_time = {DcId, TxCommitTime},
                        snapshot_time = ST
                       } = Payload,
    TxST = vectorclock:set(DcId, TxCommitTime, ST),

    case maps:take(TxId, RemainingOps) of
        {{TxStartOffset, TxOpsList}, RemainingOps1} ->
            %% Sort TxOpsList according to file order
            {RemainingOps1,
             [prepare_txn_comitted(TxId, TxST, DcId,
                                   TxStartOffset, TxCommitOffset,
                                   lists:reverse(TxOpsList))
             | FinalizedTxns]
            };
        error ->
            logger:warning("Empty transaction: ~p~n", [TxId]),
            {RemainingOps, FinalizedTxns}
    end.

prepare_txn_comitted(TxId, TxST, DcId, TxStartOffset, TxCommitOffset, TxOpsList0) ->
    logger:info("processed txn:~n ~p ~p:~p~n",
                [TxId, TxStartOffset, TxCommitOffset]),

    #wal_trans{ txid = TxId,
                dcid = DcId,
                snapshot = TxST,
                end_offset = TxCommitOffset,
                start_offset = TxStartOffset,
                materialized = TxOpsList0
              }.

prepare_txn_aborted(TxId, TxStartOffset, TxAbortOffset) ->
    #wal_trans{ txid = TxId,
                end_offset = TxAbortOffset,
                start_offset = TxStartOffset
              }.

preprocess_comitted([], LastTxId) ->
    {[], LastTxId};
preprocess_comitted([#wal_trans{txid = TxId} | _] = L, _) ->
    {lists:reverse(L), TxId}.

add_left_border(TxOffset, TxId, OrdDict) ->
    orddict:store(TxOffset, TxId, OrdDict).

remove_left_border(TxId, OrdDict) ->
    lists:keydelete(TxId, 2, OrdDict).
