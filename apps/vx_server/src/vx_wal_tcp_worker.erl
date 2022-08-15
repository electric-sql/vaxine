-module(vx_wal_tcp_worker).
-behaviour(ranch_protocol).
-behaviour(gen_server).

-export([ start_link/3,
          send/5
        ]).
-export([ init/1,
          handle_call/3,
          handle_cast/2,
          handle_info/2,
          terminate/2
        ]).

-include_lib("vx_client/include/vx_proto.hrl").
-record(state, { socket :: ranch_transport:socket(),
                 transport :: module(),
                 wal_stream :: pid() | undefined,
                 rep_id :: reference() | undefined
               }).
-type state() :: #state{}.

-spec start_link(ranch:ref(), module(), any()) -> {ok, ConnPid :: pid()}.
start_link(Ref, Transport, ProtoOpts) ->
    Pid = proc_lib:spawn_link(?MODULE, init, [{Ref, Transport, ProtoOpts}]),
    {ok, Pid}.

-spec send(port(), antidote:dcid() | undefined, antidote:txid(),
           vx_wal_stream:wal_offset(), tx_ops()) ->
          boolean() | {error, term()}.
send(Port, DcId, TxId, Offset, TxOpsList) ->
    Msg = #vx_wal_txn{ txid = TxId, dcid = DcId, wal_offset = Offset,
                       ops = TxOpsList },
    Binary = term_to_binary(Msg),
    try
        erlang:port_command(Port, Binary, [nosuspend])
    catch _:_ ->
            {error, port_closed}
    end.

init({Ref, Transport, _Opts}) ->
    {ok, Socket} = ranch:handshake(Ref),
    ok = Transport:setopts(Socket, [{packet, 4}, {active, once}]),

    gen_server:enter_loop(?MODULE, _Opts,
                          #state{socket = Socket,
                                 transport = Transport
                                }).

handle_call(_, _, State) ->
    {reply, {error, not_implemented}, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info({tcp, Socket, Data}, #state{transport = Transport} = State) ->
    try
        ok = Transport:setopts(Socket, [{active, once}]),
        handle_vx_wal_protocol(Data, State)
    of
        {ok, Req} ->
            case handle_request(Req, State) of
                {noreply, Req1, State1} ->
                    ok = Transport:send(Socket, term_to_binary(Req1)),
                    {noreply, State1};
                {error, Error} ->
                    ok = Transport:send(Socket, term_to_binary(mk_error(Error))),
                    {stop, {shutdown, Error}, State}
            end
    catch
        Type:Err:Stack ->
            logger:error("~p: ~p~n~p~n When handling request ~p~n",
                         [Type, Err, Stack, Data]),
            ok = Transport:send(Socket, term_to_binary(mk_error(bad_proto))),
            {stop, {error, bad_proto}, State}
    end;
handle_info({tcp_error, _, Reason}, #state{socket = Socket,
                                           transport = Transport,
                                           wal_stream = Pid
                                          } = State) ->
    logger:error("Socket error: ~p", [Reason]),
    Transport:close(Socket),
    _ = case Pid of
            undefined -> ok;
            _ ->
                vx_wal_stream:stop_replication(Pid)
    end,
    {stop, {error, Reason}, State};
handle_info({tcp_closed, _}, State) ->
    {stop, normal, State};

handle_info({'EXIT', _, process, Pid, Reason}, #state{wal_stream = Pid} = State) ->
    case Reason of
        normal ->
            {noreply, State#state{wal_stream = undefined}};
        _ ->
            {stop, {error, {wal_stream_crashed, Reason}}, State}
    end;
handle_info(Msg, State) ->
    logger:info("Ignored message tcp_worker: ~p~n", [Msg]),
    {noreply, State}.

terminate(_, _) ->
    ok.

%%------------------------------------------------------------------------------

handle_vx_wal_protocol(BinaryReq, _State) ->
    {ok, binary_to_term(BinaryReq)}.

-spec handle_request(term(), state()) ->
          {noreply, term(), state()} | {error, term()}.
handle_request(#vx_cli_req{ref = Ref,
                           msg = #vx_cli_start_req{opts = Opts}
                          }, State) ->
    {ok, Pid} = vx_wal_stream:start_link([]),
    case vx_wal_stream:start_replication(Pid, State#state.socket, Opts) of
        {ok, _} ->
            RepId = make_ref(),
            {noreply, #vx_srv_res{ref = Ref, msg = #vx_srv_start_res{rep_id = RepId}},
             State#state{wal_stream = Pid, rep_id = RepId}};
        {error, Reason} ->
            {noreply, #vx_srv_res{ref = Ref, msg = mk_error(Reason)}, State}
    end;
handle_request(#vx_cli_req{ref = Ref, msg = #vx_cli_stop_req{rep_id = RepId}},
               #state{wal_stream = Pid, rep_id = RepId} = State)
  when is_pid(Pid)  ->
    case vx_wal_stream:stop_replication(Pid) of
        ok ->
            {noreply, #vx_srv_res{ref = Ref, msg = #vx_srv_stop_res{}},
             State#state{wal_stream = undefined}};
        {error, Reason} ->
            {noreply, #vx_srv_res{ref = Ref, msg = mk_error(Reason)}, State}
    end;
handle_request(Msg, _State) ->
    logger:warning("bad message received: ~p~n", [Msg]),
    {error, bad_proto}.

mk_error(bad_proto) ->
    {error, bad_proto};
mk_error(_) ->
    {error, unknown_error}.
