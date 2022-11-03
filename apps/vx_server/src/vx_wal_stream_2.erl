-module(vx_wal_stream_2).
-behaviour(gen_statem).

-export([ start_link/1
          %start_replication/4,
          %stop_replication/1
        ]).

-export([ init/1,
          callback_mode/0,
          terminate/3
        ]).

-export([ init_stream/3 %,
          %await_client/3,
          %await_wal/3,
          %await_materializer/3
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
          materialized = none :: list() | none,
          global_opid :: non_neg_integer()
        }).

-record(txn_iter, { last_commit_offset :: vx_wal_utils:file_offset(),
                    last_tx_id :: antidote:txid() | undefined,
                    not_comitted_offsets = [] ::
                      orddict:orddict(non_neg_integer(), txid()),
                    not_comitted_txns = #{} :: txns_noncomitted_map(),
                    comitted_txns = []
                  }).
-type txn_iter() :: #txn_iter{}.

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

%%----------------

-spec start_link(list()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Args) ->
    gen_statem:start_link(?MODULE, Args, []).

%% @doc Callback for notification of vx_wal_stream about committed transactions.
%% Current logic would only notify the process with cast message if the status
%% is set to ready.
-spec notify_cache_update(pid(), antidote:partition_id(), antidote:dcid(),
                          antidote:op_id()) ->
          ok.
notify_cache_update(Pid, Partition, DcId, #op_number{global = OpId}) ->
    try
        logger:debug("cache update notification ~p~n~p~n",
                    [[Partition, DcId, OpId]]),
        true = ets:update_element(wal_replication_status, {Partition, DcId, Pid},
                                  { #wal_replication_status.txdata, OpId }
                                 ),
        RFun = ets:fun2ms(
                 fun(#wal_replication_status{key = {Partition0, DcId0, Pid0},
                                             notification = ready
                                            } = W)
                    when Partition0 == Partition,
                         DcId0 == DcId,
                         Pid0 == Pid->
                         W#wal_replication_status{notification = sent}
                 end),

        case ets:select_replace(wal_replication_status, RFun) of
            0 -> ok;
            1 ->
                Pid ! { partition = Partition },
                ok
        end
    catch T:E:S ->
            logger:error("Notification failed ~p:~p stack: ~p~n", [T, E, S]),
            ok
    end.

%% internal usage only

lookup_last_cache_global_opid(Partition, DcId) ->
    logging_notification_server:lookup_last_global_id(Partition, DcId).

init_notification_slot(Partition) ->
    logger:info("Init notification slot for partition ~p~n", [Partition]),
    ets:insert(wal_replication_status,
               #wal_replication_status{key = {Partition, self()},
                                       notification = ready
                                      }).

-spec mark_as_ready_for_notification(antidote:partition_id()) -> ok.
mark_as_ready_for_notification(Partition) ->
    logger:debug("Mark as ready to continue wal streaming: ~p~n", [Partition]),
    true = ets:update_element(wal_replication_status, {Partition, self()},
                              [{#wal_replication_status.notification, ready}]),
    ok.

%%----------------


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
           ?MODULE, notify_cache_update, [self()]),

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
    consume_ops_from_wal(Data).

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
            case notify_client(Data#data.port, Acc1#txn_iter.comitted_txns,
                               Data#data.send_raw_values)
            of
                {continue, _} ->
                    ok;%%consume_ops_from_wal(   );
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


notify_client(Port, Comitted, RawValues) ->
    %% try notify_client0(Port, Comitted, RawValues) of
    %%     {ok} ->
    %%         ok
    %% catch _:_ ->
    %%         ok
    %% end.
    ok.

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
    %% log are sorted according to snapshot order, which is not the case in
    %% current wal implementation. This should not cause issues in single
    %% partition case though.
    case is_materialization_ready(0, Tx) of
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

is_materialization_ready(Partition, Txn = #wal_trans{snapshot = TxST,
                                                     dcid = DcId,
                                                     global_opid = OpId
                                                    }) ->
    SN = get_stable_snapshot(),
    case compare_snapshot(SN, TxST) of
        true -> true;
        false ->
            GId = lookup_last_cache_global_opid(Partition, DcId),
            case OpId =< GId of
                true -> true;
                false -> {false, TxST, DcId, OpId}
            end
    end.

compare_snapshot(SN, TxST) ->
    try vectorclock:ge(SN, TxST) of
        Bool -> Bool
    catch _:_ ->
            false
    end.

get_stable_snapshot() ->
    case get(stable_snapshot) of
        undefined ->
            {ok, SN} = dc_utilities:get_stable_snapshot(),
            put(stable_snapshot, SN),
            SN;
        SN ->
            SN
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
    #log_record{log_operation = LogOperation, op_number = OpId} =
        log_utilities:check_log_record_version(LogRecord),

    {NotComitted1, FinalizedTxns1, NotComittedBorders1} =
        process_op0(LogOperation, NotComitted, FinalizedTxns0, TxOffset, NotComittedBorders, OpId),
    process_txns(Rest, NotComitted1, FinalizedTxns1, TxOffset, LastTxId, NotComittedBorders1).

process_op0(#log_operation{op_type = OpType, tx_id = TxId, log_payload = Payload},
           RemainingOps, FinalizedTxns, TxOffset, NotComittedBorders, _OpId)
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
           NotComittedBorders, OpId) ->
    case RemainingOps of
        #{ TxId := _ } ->
            {RemainingOps1, FinalizedTxns1} =
                process_op1(LogOp, RemainingOps, FinalizedTxns, TxOffset, OpId),
            {RemainingOps1, FinalizedTxns1, NotComittedBorders};
        _ ->
            {RemainingOps, FinalizedTxns, NotComittedBorders}
    end.

process_op1(#log_operation{op_type = OpType, tx_id = TxId, log_payload = Payload},
           RemainingOps, FinalizedTxns, _TxOffset, _OpId)
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
process_op1(#log_operation{op_type = prepare}, RemainingOps, FinalizedTxns, _TxPrepareOffset, _) ->
    {RemainingOps, FinalizedTxns};
process_op1(#log_operation{op_type = abort, tx_id = TxId}, RemainingOps, FinalizedTxns, TxAbortOffset, _) ->
    case maps:take(TxId, RemainingOps) of
        {{TxStartOffset, _}, RemainingOps1} ->
            { RemainingOps1,
              [prepare_txn_aborted(TxId, TxStartOffset, TxAbortOffset) | FinalizedTxns]
            };
        error ->
            {RemainingOps, FinalizedTxns}
    end;
process_op1(#log_operation{op_type = commit, tx_id = TxId, log_payload = Payload},
           RemainingOps, FinalizedTxns, TxCommitOffset, OpId) ->
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
                                   lists:reverse(TxOpsList),
                                   OpId
                                  )
             | FinalizedTxns]
            };
        error ->
            logger:warning("Empty transaction: ~p~n", [TxId]),
            {RemainingOps, FinalizedTxns}
    end.

prepare_txn_comitted(TxId, TxST, DcId, TxStartOffset, TxCommitOffset, TxOpsList0, OpId) ->
    logger:info("processed txn:~n ~p ~p:~p~n",
                [TxId, TxStartOffset, TxCommitOffset]),

    #wal_trans{ txid = TxId,
                dcid = DcId,
                snapshot = TxST,
                end_offset = TxCommitOffset,
                start_offset = TxStartOffset,
                materialized = TxOpsList0,
                global_opid = OpId#op_number.global
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

maybe_cancel_timer(undefined) -> ok;
maybe_cancel_timer(Ref) ->
    erlang:cancel_timer(Ref).

maybe_close_file(undefined) ->
    ok;
maybe_close_file(Fd) ->
    catch file:close(Fd).
