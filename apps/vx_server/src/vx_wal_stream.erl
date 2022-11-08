-module(vx_wal_stream).
-behaviour(gen_statem).

-export([ start_link/1,
          start_replication/4,
          stop_replication/1,
          notify_cache_update/4
        ]).

-export([ init/1,
          callback_mode/0,
          terminate/3
        ]).

-export([ init_stream/3,
          stream_catchup/3,
          await_cache/3,
          await_client/3
        ]).

-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("antidote/include/antidote.hrl").
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
          global_opid :: non_neg_integer() | undefined
        }).

-record(txn_iter, { last_commit_offset :: vx_wal_utils:file_offset(),
                    last_tx_id :: antidote:txid() | undefined,
                    not_comitted_offsets = [] ::
                      orddict:orddict(vx_wal_utils:file_offset(), txid()),
                    not_comitted_txns = #{} :: txns_noncomitted_map(),
                    comitted_txns = [] :: txns_comitted()
                  }).
-type txn_iter() :: #txn_iter{}.

-record(data, {
               await_cache :: {antidote:dcid(), non_neg_integer()} | any | undefined,

               client_pid :: pid() | undefined,
               client_mon_ref :: reference() | undefined,

               txn_iter :: txn_iter() | undefined,

               partition :: antidote:partition_id(),
               port :: port() | undefined,
               port_retry_tref :: reference() | undefined,
               port_retry_backoff :: backoff:backoff(),

               send_raw_values = false :: boolean(),

               wal_info :: #wal{} | undefined
              }).

-type txns_noncomitted_map() :: #{antidote:txid() =>
                                      { vx_wal_utils:file_offset(),
                                        [any_log_payload()]
                                      }
                                 }.
-type txns_comitted() :: [ #wal_trans{} ].

-type wal_offset() :: {vx_wal_utils:file_offset(), vx_wal_utils:file_offset()}.

-export_type([wal_offset/0]).

-spec start_link(list()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Args) ->
    gen_statem:start_link(?MODULE, Args, []).

%% @doc Starts replication, Port is expected to be a tcp port, opened by
%% vx_wal_tcp_worker.
-spec start_replication(pid(), reference(), port(),
                        [{offset, 0 | eof | wal_offset()} |
                         {raw_values, true | false}
                        ]) ->
          {ok, reference()} | {error, term()}.
start_replication(Pid, ReqRef, Port, RepOpts) ->
    gen_statem:call(Pid, {start_replication, ReqRef, Port, RepOpts}, infinity).

%% @doc Ask vx_wal_stream to stop replicating of the data.
-spec stop_replication(pid()) -> ok | {error, term()}.
stop_replication(Pid) ->
    gen_statem:call(Pid, {stop_replication}).

%% @doc Callback for notification of vx_wal_stream about committed transactions.
%% Current logic would only notify the process with cast message if the status
%% is set to ready.
-spec notify_cache_update(pid(), antidote:partition_id(), antidote:dcid(),
                          antidote:op_number()) ->
          ok.
notify_cache_update(Pid, Partition, DcId, #op_number{global = OpId}) ->
    logger:debug("cache update notification ~p~n",
                    [[Partition, DcId, OpId]]),
    Pid ! {cache, DcId, OpId},
    ok.

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

init_stream({call, Sender}, {stop_replication}, Data) ->
    {keep_state, Data, [{reply, Sender, {error, no_active_replication} }]};

init_stream(Type, Msg, Data) ->
    common_callback(Type, Msg, Data).

stream_catchup(internal, continue_stream, Data) ->
    consume_ops_from_wal(Data);
stream_catchup(Type, Msg, Data) ->
    common_callback(Type, Msg, Data).

await_cache(info, {cache, _DcId, _OpId}, #data{await_cache = any} = Data) ->
    consume_ops_from_wal(Data);
await_cache(info, {cache, DcId, OpId1}, #data{await_cache = {DcId, OpId2}} = Data) ->
    case OpId1 >= OpId2 of
        true ->
            continue_client_stream(Data#data.txn_iter, Data#data.wal_info,
                                   Data#data{await_cache = any}
                                  );
        false ->
            keep_state_and_data
    end;
await_cache(info, {cache, _DcId, _OpId}, _Data) ->
    keep_state_and_data;
await_cache(Type, Msg, Data) ->
    common_callback(Type, Msg, Data).

await_client(info, {cache, _, _} = Msg, Data) ->
    logger:debug("ignore cache notification ~p ~p~n", [Msg, Data#data.await_cache]),
    keep_state_and_data;
await_client(info, {timeout, TRef, port_send_retry}, Data = #data{port_retry_tref = TRef}) ->
    logger:debug("tcp port retry: ~p~n", [Data#data.port_retry_backoff]),
    continue_client_stream(Data#data.txn_iter, Data#data.wal_info,
                                   Data#data{await_cache = any}
                                  );
await_client(Type, Msg, Data) ->
    common_callback(Type, Msg, Data).

common_callback({call, Sender}, {stop_replication}, Data) ->
    logger:info("request to stop replication~n", []),

    _ = maybe_close_file(Data#data.wal_info),
    _ = maybe_cancel_timer(Data#data.port_retry_tref),

    ok = logging_notification_server:delete_handler([]),

    {next_state, init_stream,
     Data#data{await_cache = undefined,
               port_retry_tref = undefined,
               txn_iter = undefined,
               wal_info = undefined
              },
     [{reply, Sender, ok}]
    };

common_callback({call, _Sender}, {start_replication, _, _, _}, Data) ->
    {stop, {shutdown, wrong_state}, Data};

common_callback(info, {'DOWN', _Mref, process, Pid, _}, #data{client_pid = Pid} = Data) ->
    {stop, normal, Data};

common_callback(info, {gen_event_EXIT, _Handler, _Reason}, Data) ->
    {keep_state, Data};

common_callback(info, {inet_reply, _Sock, ok}, _Data) ->
    keep_state_and_data;

common_callback(info, {inet_reply, _Sock, {error, Reason}}, Data) ->
    logger:error("socket error: ~p~n", [Reason]),
    {stop, {shutdown, Reason}, Data};

common_callback(info, {'EXIT', Sock, Reason}, Data = #data{port = Sock}) ->
    logger:error("socket closed: ~p~n", [Reason]),
    {stop, {shutdown, Reason}, Data};

common_callback(info, {timeout, _TRef, port_send_retry}, _) ->
    keep_state_and_data;

common_callback(info, {cache, _DcId, _OpId} = Msg, _Data) ->
    %% Not interested in cache updates any more
    logger:debug("ignore cache notification ~p~n", [Msg]),
    keep_state_and_data.


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
        {ok, TxIter1, Wal1} -> %% We reached end of WAL log
            %% Let's wait for notifications from cache system
            {next_state, await_cache, Data#data{await_cache = any,
                                                txn_iter = TxIter1,
                                                wal_info = Wal1
                                               }};
        {stop, TxIter1, Wal1} -> %% There is data to send
            continue_client_stream(TxIter1, Wal1, Data);
        {error, port_closed} ->
            logger:info("port closed~n"),
            {stop, {shutdown, port_closed}};
        {error, _} = Error ->
            {stop, Error}
    end.

continue_client_stream(TxIter1, Wal1, Data) ->
    case notify_client(TxIter1#txn_iter.comitted_txns, TxIter1, Data) of
        {continue, TxIter2} ->
            consume_ops_from_wal(TxIter2, Wal1, Data);
        {client_stop, _Tx, TxnIter2} ->
                    %% Poll the client, or wait for async confirmation
            {next_state, await_client, set_port_send_timer(Data#data{wal_info = Wal1,
                                                                     txn_iter = TxnIter2
                                                                    })};
        {not_ready, Tx, TxnIter2} ->
            %% Means that the cache is not ready yet
            Op = Tx#wal_trans.global_opid,
            DcId = Tx#wal_trans.dcid,
            %% We can not materialize the data
            {next_state, await_cache, Data#data{await_cache = {DcId, Op},
                                                wal_info = Wal1,
                                                txn_iter = TxnIter2
                                               }};
        {error, port_closed} ->
            logger:info("port closed~n"),
            {stop, {shutdown, port_closed}}
        %{error, _} = Error ->
        %    {stop, Error}
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

-spec notify_client(txns_comitted(), #txn_iter{}, #data{}) ->
          {error, {shutdown, term()} | term()} | {not_ready, #wal_trans{}, #txn_iter{}} |
          {client_stop, #wal_trans{}, #txn_iter{}} | {continue, #txn_iter{}}.
notify_client(ComittedTxns, TxnIter, Data) ->
   notify_client0(ComittedTxns, TxnIter, Data).

notify_client0([#wal_trans{materialized = none} | FinalyzedTxns], TxnIter, Data) ->
    notify_client0(FinalyzedTxns, TxnIter, Data);
notify_client0([#wal_trans{txid = TxId, end_offset = EOffset} | FinalyzedTxns],
               #txn_iter{last_commit_offset = RB, not_comitted_offsets = LB} = TxnIter, Data)
  when EOffset =< RB ->
    logger:info("skip transaction with commit offset: ~p~n", [EOffset]),
    LB1 = remove_left_border(TxId, LB),
    notify_client0(FinalyzedTxns, TxnIter#txn_iter{not_comitted_offsets = LB1}, Data);
notify_client0([#wal_trans{} = Tx | FinalizedTxns] = Txns,
               #txn_iter{not_comitted_offsets = LB} = TxIter,
               #data{send_raw_values = RawValues, port = Port} = Data) ->
    %% FIXME: We have a general assumption here, that transactions in the WAL
    %% log are sorted according to snapshot order, which is not the case in
    %% current wal implementation. This should not cause issues in single
    %% partition case though.
    case is_materialization_ready(Data#data.partition, Tx) of
        true ->
            case notify_client1(Tx, Port, LB, RawValues) of
                ok ->
                    LB1 = remove_left_border(Tx#wal_trans.txid, LB),
                    notify_client0(FinalizedTxns, TxIter#txn_iter{not_comitted_offsets = LB1}, Data);
                {error, _} = Error ->
                    Error;
                false ->
                    {client_stop, Tx, TxIter#txn_iter{comitted_txns = Txns}}
            end;
        false ->
            {not_ready, Tx, TxIter#txn_iter{comitted_txns = Txns}}
    end;
notify_client0([], TxIter, _Data) ->
    {continue, TxIter#txn_iter{comitted_txns = []}}.

notify_client1(#wal_trans{txid = TxId,
                          dcid = DcId,
                          snapshot = TxST,
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

is_materialization_ready(Partition, #wal_trans{snapshot = TxST, dcid = DcId, global_opid = OpId}) ->
    SN = get_stable_snapshot(),
    case compare_snapshot(SN, TxST) of
        true -> true;
        false ->
            GId = lookup_last_cache_global_opid(Partition, DcId),
            OpId =< GId
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

lookup_last_cache_global_opid(Partition, DcId) ->
    logging_notification_server:lookup_last_global_id(Partition, DcId).


%%-----------------------------------------------------------------------------
%% Transactions processing
%%-----------------------------------------------------------------------------


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
            logger:warning("empty transaction: ~p~n", [TxId]),
            {RemainingOps, FinalizedTxns}
    end.

prepare_txn_comitted(TxId, TxST, DcId, TxStartOffset, TxCommitOffset, TxOpsList0, OpId) ->
    logger:info("srv wal stream txn:~n ~p ~p:~p~n",
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
maybe_close_file(#wal{} = Wal) ->
    vx_wal_utils:close_wal(Wal).

set_port_send_timer(Data) ->
    Data#data{port_retry_tref = backoff:fire(Data#data.port_retry_backoff)}.
