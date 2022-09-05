%%------------------------------------------------------------------------------
%% @doc Module that encapsulates logic for WAL replication.
%% Current known limitations for this module:
%% - no streaming support for replication of multiple partitions
%% - we may have a race condition when we try to materialize the value that
%%   have not been updated in materializer yet but have been persisted in the
%%   wal Stable snapshot notification service would be able to fix this problem
%%------------------------------------------------------------------------------

-module(vx_wal_stream).
-behaviour(gen_statem).

-export([ start_link/1,
          start_replication/4,
          stop_replication/1,
          notify_commit/5
        ]).

-export([ init/1,
          init_stream/3,
          await_data/3,
          callback_mode/0,
          terminate/3,
          code_change/4
        ]).

%% Extracted from:
%% -include_lib("kernel/src/disk_log.hrl").
-record(continuation,
        {pid = self() :: pid(),
         %% We only know how to handle halt logs here, so silent dialyzer
         pos          :: non_neg_integer(), %% | {integer(), non_neg_integer()},
         b            :: binary() | [] | pos_integer()
        }).

-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("antidote/include/antidote.hrl").
-include_lib("antidote/include/meta_data_notif_server.hrl").
-include("vx_wal_stream.hrl").

-define(POLL_RETRY_MIN, 10).
-define(POLL_RETRY_MAX, timer:seconds(10)).
-define(HEADERSZ, 8).

-record(wal_trans,
        { txid :: antidote:txid(),
          %% dcid is not determined for aborted transactions
          dcid :: antidote:dcid() | undefined,
          %% Wal offset, that points to first transaction record that
          %% contains update_start.
          start_offset :: file_offset(),
          %% Wal offset, that points to position before commit record
          end_offset :: file_offset(),
          %% Snapshot here is snapshot of transaction updated with
          %% commit timestamp of the transaction
          snapshot = none :: antidote:snapshot_time() | none,
          %% Materialized is left with none for aborted transactions
          materialized = none :: list() | none
        }).

-record(data, {client :: pid() | undefined,
               mon_ref:: reference() | undefined,
               file_status :: more_data | eof,
               file_desc :: file:fd() | undefined,
               file_name :: file:filename_all(),
               file_buff = [] :: term(),
               file_pos = 0 :: non_neg_integer(),
               txns_buffer :: txns_noncomitted_map(),
               %% Last read TxId. May be either comitted or aborted
               last_read_txid :: antidote:txid() | undefined,
               %% Last txid we have been notified about
               last_notif_txid :: antidote:txid() | undefined,

               %% Start offset position, based on not-send transactions
               left_offset_border = [] ::
                 orddict:orddict(non_neg_integer(), txid()),
               %% Commit offset position, based on last send transaction
               right_offset_border = 0 :: non_neg_integer(),

               %% Retry for file polling
               file_poll_tref :: reference() | undefined,
               file_poll_backoff :: backoff:backoff(),
               partition :: antidote:partition_id(),

               opts_raw_values = false :: boolean(),
               %% Buffer with materialized transasction that have not
               %% been sent
               to_send = [],
               %% Retry backoff for tcp port send
               port_retry_tref :: reference() | undefined,
               port_retry_backoff :: backoff:backoff(),
               port :: port() | undefined
              }).

-type log_position() :: {non_neg_integer(), Buffer :: term()}.
-type file_offset() :: non_neg_integer().
-type wal_offset() :: {file_offset(), file_offset()}.

-type txns_noncomitted_map() :: #{antidote:txid() =>
                                      { file_offset(), [any_log_payload()]}
                                 }.
-type txns_comitted() :: [ { antidote:txid(),
                             antidote:dcid(),
                             antidote:clock_time(),
                             file_offset(),
                             term() } ].
-type state() :: #data{}.

-record(commit, { partition :: antidote:partition_id(),
                  txid :: antidote:txid(),
                  snapshot :: snapshot_time()
                }).

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
-spec notify_commit(pid(), antidote:partition_id(), antidote:txid(),
                    antidote:clock_time(), antidote:snapshot_time()
                   ) ->
          ok.
notify_commit(Pid, Partition, TxId, CommitTime, SnapshotTime) ->
    try
        logger:info("commit notification ~p~n~p~n",
                    [TxId, [Partition, TxId, CommitTime, SnapshotTime]]),
        true = ets:update_element(wal_replication_status, {Partition, Pid},
                           { #wal_replication_status.txdata,
                             {TxId, CommitTime, SnapshotTime}
                           }),
        RFun = ets:fun2ms(
                 fun(#wal_replication_status{key = {Partition0, Pid0},
                                             notification = ready
                                            } = W)
                    when Partition0 == Partition,
                         Pid0 == Pid->
                         W#wal_replication_status{notification = sent}
                 end),

        case ets:select_replace(wal_replication_status, RFun) of
            0 -> ok;
            1 ->
                Pid ! #commit{ partition = Partition, txid = TxId,
                               snapshot = SnapshotTime
                             },
                ok
        end
    catch T:E:S ->
            logger:error("Notification failed ~p:~p stack: ~p~n", [T, E, S]),
            ok
    end.

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

callback_mode() ->
    state_functions.

init(_Args) ->
    %% FIXME: Only work with a single partition at the moment
    %% FIXME: Move this initialization back to start_replication
    [Partition] = dc_utilities:get_all_partitions(),
    %% We would like to know where the file is located
    InfoList = [_|_] = disk_log:info(log_path(Partition)),
    LogFile = proplists:get_value(file, InfoList),
    halt    = proplists:get_value(type, InfoList), %% Only handle halt type of the logs

    FilePollBackoff0 = backoff:init(?POLL_RETRY_MIN, ?POLL_RETRY_MAX,
                                    self(), file_poll_retry),
    FilePollBackoff1 = backoff:type(FilePollBackoff0, jitter),

    PortSendBackoff0 = backoff:init(?POLL_RETRY_MIN, ?POLL_RETRY_MAX,
                                    self(), port_send_retry),
    PortSendBackoff1 = backoff:type(PortSendBackoff0, jitter),

    ok = vx_wal_stream_server:register(),
    {ok, init_stream, #data{file_name = LogFile,
                            txns_buffer = #{},
                            partition = Partition,
                            file_poll_backoff = FilePollBackoff1,
                            port_retry_backoff = PortSendBackoff1,
                            file_status = more_data
                           }}.

%% Copied from logging_vnode and simplified for our case
-spec log_path(antidote:partition_id()) -> file:filename().
log_path(Partition) ->
    LogFile = integer_to_list(Partition),
    {ok, DataDir} = application:get_env(antidote, data_dir),
    LogId = LogFile ++ "--" ++ LogFile,
    filename:join(DataDir, LogId).

init_stream({call, {Sender, _} = F}, {start_replication, ReqRef, Port, Opts}, Data) ->
    %% FIXME: We support only single partition for now
    {LWalOffset0, RWalOffset0} =
        case proplists:get_value(offset, Opts, none) of
            0    -> {0, 0};
            none -> {0, 0};
            eof  -> {eof, 0};
            {LOffset, ROffset} -> {LOffset, ROffset}
        end,
    {ok, FD, FileCurOffset} = open_log(Data#data.file_name, LWalOffset0),
    MonRef = erlang:monitor(process, Sender),

    true = init_notification_slot(Data#data.partition),
    ok = logging_notification_server:add_handler(
           ?MODULE, notify_commit, [self()]),
    {_, Backoff} = backoff:succeed(Data#data.file_poll_backoff),

    ReplicationRef = make_ref(),
    ok = vx_wal_tcp_worker:send_start(Port, ReqRef, ReplicationRef),

    {next_state, await_data, Data#data{client = Sender,
                                       mon_ref = MonRef,
                                       file_pos = FileCurOffset,
                                       file_buff = [],
                                       file_desc = FD,
                                       file_poll_backoff = Backoff,
                                       right_offset_border = RWalOffset0,
                                       port = Port,
                                       opts_raw_values
                                           = proplists:get_bool(raw_values, Opts)
                                      },
     [{state_timeout, 0, {timeout, undefined, file_poll_retry} },
      {reply, F, {ok, ReplicationRef}}]};

init_stream(info, {gen_event_EXIT, _Handler, _Reason}, Data) ->
    {keep_state, Data};

init_stream(info, {inet_reply, _Sock, ok}, _Data) ->
    keep_state_and_data;

init_stream(info, {inet_reply, _Sock, {error, Reason}}, Data) ->
    logger:error("socket error: ~p~n", [Reason]),
    {stop, {shutdown, Reason}, Data}.

await_data(_, #commit{txid = TxId}, #data{file_poll_tref = undefined} = Data) ->
    continue_wal_reading(Data#data{last_notif_txid = TxId});

await_data(_, #commit{}, Data) ->
    {keep_state, Data};

await_data(_, {timeout, TRef, file_poll_retry}, Data = #data{file_poll_tref = TRef}) ->
    logger:debug("file poll retry: ~p~n", [Data#data.file_poll_backoff]),
    continue_wal_reading(Data#data{file_poll_tref = undefined});

await_data(_, {timeout, TRef, port_send_retry}, Data = #data{port_retry_tref = TRef}) ->
    logger:debug("tpc port retry: ~p~n", [Data#data.port_retry_backoff]),
    {ok, SN} = dc_utilities:get_stable_snapshot([include_local_dc]),
    continue_send(SN, Data#data{port_retry_tref = undefined});

await_data({call, Sender}, {stop_replication}, Data) ->
    _ = file:close(Data#data.file_desc),
    ok = logging_notification_server:delete_handler([]),
    erlang:demonitor(Data#data.mon_ref),
    {_, FBackoff} = backoff:succeed(Data#data.file_poll_backoff),
    {_, PBackoff} = backoff:succeed(Data#data.port_retry_backoff),
    _ = case Data#data.file_poll_tref of
            undefined -> ok;
            TRef0 -> erlang:cancel_timer(TRef0)
        end,
    _ = case Data#data.port_retry_tref of
            undefined -> ok;
            TRef1 -> erlang:cancel_timer(TRef1)
        end,
    {next_state, init_stream, #data{file_name = Data#data.file_name,
                                    file_status = more_data,
                                    txns_buffer = #{},
                                    partition = Data#data.partition,
                                    file_poll_backoff = FBackoff,
                                    port_retry_backoff = PBackoff
                                   },
     [{reply, Sender, ok}]
    };

await_data({call, Sender}, Msg, Data) ->
    logger:info("Ignored message: ~p~n", [Msg]),
    {keep_state, Data, [{reply, Sender, {error, unhandled_msg}}] };

await_data(info, {gen_event_EXIT, _Handler, _Reason}, Data) ->
    logger:error("Logging notification server terminated accidently, stopping"),
    {stop, {shutdown, {logging_notification_server, terminated}}, Data};

%% We receive inet_reply messages due to port_command/3 nosuspend
%% call in vx_wal_tcp_worker:send/2
await_data(info, {inet_reply, _Sock, Reason}, Data) ->
    case Reason of
        ok ->
            keep_state_and_data;
        {error, EReason} ->
            logger:error("socket error: ~p~n", [EReason]),
            {stop, {shutdown, Reason}, Data}
    end;

await_data(info, #md_stable_snapshot{sn = Sn}, Data) ->
    %% We received notification about snapshot being stable for some of the
    %% transasctions we couldn't send previously
    logger:debug("stable notification: ~p~n", [Sn]),
    continue_send(Sn, Data);

await_data(_, Msg, _Data) ->
    logger:info("Ignored message wal streamer: ~p~n", [Msg]),
    keep_state_and_data.

%%------------------------------------------------------------------------------

continue_send(SN, #data{} = Data) ->
    case notify_client(SN, Data#data.to_send, Data) of
        {ok, Data1} when Data1#data.file_status == more_data ->
            continue_wal_reading(Data1);
        {retry, Data1} ->
            {keep_state, Data1};
        {error, _Reason} = Error ->
            {stop, Error}
    end.

continue_wal_reading(#data{partition = Partition} = Data) ->
    logger:info("Continue wal streaming for client ~p on partition ~p"
                " at position ~p~n",
                [Data#data.client, Data#data.partition, Data#data.file_pos]),

    case read_ops_from_log(Data) of
        {ok, Data1} when Data1#data.file_status == more_data ->
            {next_state, await_data, Data1};
        {ok, Data1} when Data1#data.file_status == eof ->
            ok = mark_as_ready_for_notification(Partition),
            case fetch_latest_position(Partition) of
                undefined ->
                    {next_state, await_data, Data1};
                {TxId, _, _} when TxId == Data1#data.last_read_txid ->
                    %% All the data is read, no need to set the timer
                    {next_state, await_data, Data1#data{last_notif_txid = TxId}};
                {TxId, _, _} when TxId == Data1#data.last_notif_txid ->
                    %% No matter whether or not we read something new, we didn't
                    %% read TxId transaction, so set the poll timer based on
                    %% backoff value. That may happen if WAL is not immediatly
                    %% synced after transasction is committed.
                    {next_state, await_data, set_file_poll_timer(Data1)};
                {TxId, _, _} ->
                    %% New TxId since we last checked. For now I would like
                    %% to keep it as a separate case here.
                    {next_state, await_data,
                     set_file_poll_timer(Data1#data{last_notif_txid = TxId})}
            end;
        {error, _} = Error ->
            {stop, Error}
    end.

set_file_poll_timer(Data) ->
    Data#data{file_poll_tref = backoff:fire(Data#data.file_poll_backoff)}.

set_port_send_timer(Data) ->
    Data#data{port_retry_tref = backoff:fire(Data#data.port_retry_backoff)}.


-spec fetch_latest_position(antidote:partition_id()) -> undefined |
          {antidote:txid(), antidote:clock_time(), antidote:snapshot_time()}.
fetch_latest_position(Partition) ->
    ets:lookup_element(wal_replication_status,
                       {Partition, self()}, #wal_replication_status.txdata).

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.

-spec open_log(file:filename_all(), integer() | eof) ->
          {ok, file:fd(), pos_integer() }.
open_log(LogFile, WalPos) ->
    {ok, FD} = file:open(LogFile, [raw, binary, read]),
    case WalPos of
        0 ->
            {ok, _Head} = file:read(FD, ?HEADERSZ),
            {ok, FD, 0};
        eof ->
            {ok, NPos} = file:position(FD, eof),
            {ok, FD, NPos - ?HEADERSZ};
        WalPos when is_integer(WalPos) ->
            {ok, NPos} = file:position(FD, WalPos),
            {ok, FD, NPos}
    end.

read_ops_from_log(Data) ->
    case read_ops_from_log(undefined, Data, 0) of
        {ok, 0, Data1} ->
            {_, Backoff} = backoff:fail(Data1#data.file_poll_backoff),
            {ok, Data1#data{file_poll_backoff = Backoff}};
        {ok, _, Data1} ->
            {_, Backoff} = backoff:succeed(Data1#data.file_poll_backoff),
            {ok, Data1#data{file_poll_backoff = Backoff}};
        {error, _} = Error ->
            Error
    end.

read_ops_from_log(SN, #data{txns_buffer = TxNotComitted0,
                        file_pos = FPos,
                        file_buff = FBuff,
                        file_desc = Fd
                       } = Data, N) ->
    case read_ops_from_log(Fd, Data#data.file_name, FPos, FBuff) of
        %% IF we received eof here, that means we haven't read anything from the
        %% log during this call
        {eof, LogPosition, []} ->
            {FPos1, FBuff1} = LogPosition,
            {ok, N, Data#data{file_pos = FPos1,
                              file_buff = FBuff1,
                              file_status = eof
                             }};
        {error, _} = Error ->
            Error;
        {ok, {FPos1, FBuff1}, NewTerms} ->
            %% Calculate position before next chunk was obtained
            TxOffset = generate_pos_in_log({FPos, FBuff}),
            logger:debug("~p offset reported: ~p~n", [self(), TxOffset]),

            {ok, ComittedData, Data1} =
                process_txns(NewTerms, TxNotComitted0, [], TxOffset,
                             Data#data{file_pos = FPos1,
                                       file_buff = FBuff1,
                                       file_status = more_data
                                       }
                            ),
            %% We try to postpone get_stable_snapshot here
            {ok, SnStable}
                = case SN of
                      undefined -> dc_utilities:get_stable_snapshot(
                                     [include_local_dc]);
                      _ ->
                          {ok, SN}
                  end,
            case notify_client(SnStable, ComittedData, Data1) of
                {ok, Data2} ->
                    read_ops_from_log(SnStable, Data2, N+1);
                {retry, Data2} ->
                    {ok, N + 1, Data2};
                {error, _} = Error ->
                    Error
            end
    end.

-spec read_ops_from_log(file:fd(), file:filename_all(), non_neg_integer(), term()) ->
          {error, term()} |
          {ok | eof, log_position(), [term()]}.
read_ops_from_log(Fd, FileName, FPos, FBuffer) ->
    case read_chunk(Fd, FileName, FPos, FBuffer, 1) of
        {eof, LogPosition, []} ->
            {eof, LogPosition, []};
        {error, _} = Error ->
            Error;
        {ok, LogPosition, NewTerms}->
            {ok, LogPosition, NewTerms}
    end.

%% If we have emitted new terms -> we can consider offset to be Pos - size(Binary) - headersz
%% If we have not emitted new terms -> we do not update offset, as we expect that the
%% chunk was larger then the MAX_CHUNK_SIXE in disk_log
%% If eof we simple retry with the buffe and last settings
generate_pos_in_log({FPos, FBuf}) ->
    case FBuf of
        [] -> FPos;
        B  ->
            FPos - byte_size(B)
    end.

-spec read_chunk(file:fd(), file:filename_all(), non_neg_integer(), term(), non_neg_integer()) ->
          {ok, log_position(), [ {term(), #log_record{}} ]} |
          {error, term()} |
          {eof, log_position(), [ term() ]}.
read_chunk(Fd, FileName, FPos, FBuff, Amount) ->
    R = disk_log_1:chunk_read_only(Fd, FileName, FPos, FBuff, Amount),
    %% Create terms from the binaries returned from chunk_read_only/5.
    %% 'foo' will do here since Log is not used in read-only mode.
    case disk_log:ichunk_end(R, _Log = foo) of
        {#continuation{pos = FPos1, b = Buffer1}, Terms}
          when FPos == FPos1 ->
            %% The same block but different term position
            {ok, {FPos1, Buffer1}, Terms};
        {#continuation{pos = FPos1, b = Buffer1}, Terms} ->
            {ok, {FPos1, Buffer1}, Terms};
        {#continuation{}, [], _BadBytes} = R ->
            logger:error("Bad bytes: ~p~n", [R]),
            {error, badbytes};
        {error, _} = Error ->
            Error;
        eof ->
            %% That's ok, just need to keep previous position
            {eof, {FPos, FBuff}, []}
    end.

-spec process_txns([{term(), #log_record{}}], txns_noncomitted_map(), txns_comitted(),
                   file_offset(), state()) ->
          {ok, txns_comitted(), state()}.
process_txns([], TxnsNonComitted, FinalizedTxns, _, Data) ->
    {Txns, LastTxId} = preprocess_comitted(FinalizedTxns,
                                           Data#data.last_read_txid),
    {ok, Txns, Data#data{ txns_buffer = TxnsNonComitted,
                          last_read_txid = LastTxId
                        }};
process_txns([{_, LogRecord} | Rest], TxnsNonComitted0, FinalizedTxns0, TxOffset, Data) ->
    #log_record{log_operation = LogOperation} =
        log_utilities:check_log_record_version(LogRecord),

    {TxnsNonComitted1, FinalizedTxns1, Data1} =
        process_op0(LogOperation, TxnsNonComitted0, FinalizedTxns0, TxOffset, Data),
    process_txns(Rest, TxnsNonComitted1, FinalizedTxns1, TxOffset, Data1).

process_op0(#log_operation{op_type = OpType, tx_id = TxId, log_payload = Payload},
           RemainingOps, FinalizedTxns, TxOffset, Data)
  when OpType == update_start ->
    {Key, Type, Op} = { Payload#update_log_payload.key,
                        Payload#update_log_payload.type,
                        Payload#update_log_payload.op
                      },
    RemainingOps1 = RemainingOps#{ TxId => {TxOffset, [{Key, Type, Op}]} },
    %% Record the starting offset of the new transaction
    Data1 = add_left_border(TxOffset, TxId, Data),
    {RemainingOps1, FinalizedTxns, Data1};
process_op0(LogOp = #log_operation{tx_id = TxId}, RemainingOps, FinalizedTxns, TxOffset, Data) ->
    case RemainingOps of
        #{ TxId := _ } ->
            process_op1(LogOp, RemainingOps, FinalizedTxns, TxOffset, Data);
        _ ->
            {RemainingOps, FinalizedTxns, Data}
    end.

process_op1(#log_operation{op_type = OpType, tx_id = TxId, log_payload = Payload},
           RemainingOps, FinalizedTxns, _TxOffset, Data)
  when OpType == update ->
    {Key, Type, Op} = { Payload#update_log_payload.key,
                        Payload#update_log_payload.type,
                        Payload#update_log_payload.op
                      },
    RemainingOps1 =
                maps:update_with(TxId, fun({Offset, Ops}) ->
                                               {Offset, [{Key, Type, Op} | Ops]}
                                       end, RemainingOps),
    {RemainingOps1, FinalizedTxns, Data};
process_op1(#log_operation{op_type = prepare}, RemainingOps, FinalizedTxns, _TxPrepareOffset, Data) ->
    {RemainingOps, FinalizedTxns, Data};
process_op1(#log_operation{op_type = abort, tx_id = TxId}, RemainingOps, FinalizedTxns, TxAbortOffset, Data) ->
    case maps:take(TxId, RemainingOps) of
        {{TxStartOffset, _}, RemainingOps1} ->
            { RemainingOps1,
              [prepare_txn_aborted(TxId, TxStartOffset, TxAbortOffset) | FinalizedTxns],
              Data
            };
        error ->
            {RemainingOps, FinalizedTxns, Data}
    end;
process_op1(#log_operation{op_type = commit, tx_id = TxId, log_payload = Payload},
           RemainingOps, FinalizedTxns, TxCommitOffset, Data) ->
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
             | FinalizedTxns],
             Data
            };
        error ->
            logger:warning("Empty transaction: ~p~n", [TxId]),
            {RemainingOps, FinalizedTxns, Data}
    end.

prepare_txn_comitted(TxId, TxST, DcId, TxStartOffset, TxCommitOffset, TxOpsList0) ->
    logger:info("processed txn:~n ~p ~p:~p ~p~n",
                [TxId, TxStartOffset, TxCommitOffset, TxOpsList0]),

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

notify_client(SnStable, FinalyzedTxns,
              Data = #data{port = Port,
                           right_offset_border = RB,
                           last_notif_txid = LastSendTx,
                           left_offset_border = LB,
                           opts_raw_values = RawValuesB
                          }) ->
    case
        notify_client0(SnStable, FinalyzedTxns, LastSendTx, Port, LB, RB, RawValuesB)
    of
        {ok, LastTxId, LB1, RB1} ->
            {ok, Data#data{last_notif_txid = LastTxId,
                           left_offset_border = LB1,
                           right_offset_border = RB1,
                           to_send = []
                          }};
        {retry, NotSendTxns, LastTxId, LB1, RB1} ->
            {retry, set_port_send_timer(
                      Data#data{to_send = NotSendTxns,
                                last_notif_txid = LastTxId,
                                left_offset_border = LB1,
                                right_offset_border = RB1
                               })};
        {retry2, TxSt, NotSendTxns, LastTxId, LB1, RB1} ->
            ok = meta_data_notif_server:subscribe_to_stable(TxSt),
            {retry, Data#data{to_send = NotSendTxns,
                              last_notif_txid = LastTxId,
                              left_offset_border = LB1,
                              right_offset_border = RB1
                             }};
        {error, Reason} ->
            {error, Reason}
    end.

notify_client0(SN, [#wal_trans{materialized = none} | FinalyzedTxns],
               LastSentTx, Port, LB, RB, RawValuesB) ->
    notify_client0(SN, FinalyzedTxns, LastSentTx, Port, LB, RB, RawValuesB);
notify_client0(SN, [#wal_trans{txid = TxId, end_offset = EOffset} | FinalyzedTxns],
               LastSentTx, Port, LB, RB, RawValuesB)
  when EOffset < RB ->
    logger:debug("skip transaction with commit offset: ~p~n", [EOffset]),
    notify_client0(SN, FinalyzedTxns, LastSentTx, Port,
                   remove_left_border(TxId, LB), RB, RawValuesB);
notify_client0(SN, [#wal_trans{snapshot = TxST} | FinalyzedTxns] = Txns, LastTxn,
               Port, LB, RightOffset, RawValuesB) ->
    %% FIXME: We have a general assumption here, that transactions in the WAL
    %% log are sorted according to snapshot order, which is not the case in current
    %% wal implementation. This should not cause issues in single partition case
    %% though.
    try vectorclock:ge(SN, TxST) of
        true ->
            notify_client1(SN, Txns, LastTxn,
                           Port, LB, RightOffset, RawValuesB);
        false ->
            {retry2, TxST, Txns, LastTxn, LB, RightOffset}
    catch
        _:_ ->
            {retry2, TxST, Txns, LastTxn, LB, RightOffset}
    end;
notify_client0(_, [], LastTxn, _, LB, RB, _) ->
    {ok, LastTxn, LB, RB}.


notify_client1(SN, [#wal_trans{txid = TxId, dcid = DcId, snapshot = TxST,
                               materialized = TxOpsList,
                               start_offset = _SOffset,
                               end_offset = EOffset
                              } | FinalyzedTxns] = Txns, LastTxn, Port, LB, RightOffset,
               RawValuesB
              ) ->
    LeftOffset = case LB of
                     [] -> 0;
                     [{LeftOff, _} | _] -> LeftOff
                 end,
    TxOffset = {LeftOffset, EOffset},
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
            {retry, Txns, LastTxn, LB, RightOffset};
        true ->
            notify_client0(SN, FinalyzedTxns, TxId, Port,
                           remove_left_border(TxId, LB), EOffset, RawValuesB);
        {error, Reason} ->
            {error, Reason}
    end.

add_left_border(TxOffset, TxId, Data = #data{left_offset_border = OrdDict}) ->
    Data#data{ left_offset_border = orddict:store(TxOffset, TxId, OrdDict) }.

remove_left_border(TxId, OrdDict) ->
    lists:keydelete(TxId, 2, OrdDict).

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
