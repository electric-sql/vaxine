-module(vx_client_socket).
-behaviour(gen_server).

-export([start_link/2,
         start_link/3,
         disconnect/1,
         stop/1
        ]).

-export([start_replication/2,
         get_next_stream_bulk/2,
         stop_replication/1
        ]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2]).

-define(CONN_TIMEOUT, 1000).
-define(RECONN_MIN, 500).
-define(RECONN_MAX, 5000). %% 5 seconds

-include("vx_proto.hrl").

-type address() :: inet:socket_address() | inet:hostname().

-record(state, { address :: address(),
                 port :: inet:port_number(),
                 opts :: [ gen_tcp:connect_option()
                        % | inet:inet_backend()
                         ],
                 socket :: gen_tcp:socket() | undefined,
                 socket_status = disconnected :: disconnected | connected,
                 connect_timeout = ?CONN_TIMEOUT :: non_neg_integer(),
                 reconnect_backoff = backoff:backoff(),
                 reconnect_tref :: reference() | undefined,
                 wal_replication :: requested | reference() | undefined,
                 owner :: pid(),
                 last_req_id :: reference() | undefined,
                 owner_req :: term() | undefined
               }).

-spec start_link(address(), inet:port_number()) ->
          {ok, pid()} | {error, term()}.
start_link(Address, Port) ->
    start_link(Address, Port, []).

-spec start_link(address(), inet:port_number(), list()) ->
          {ok, pid()} | {error, term()}.
start_link(Address, Port, Options) when is_list(Options) ->
    gen_server:start_link(?MODULE, [Address, Port, Options, self()], []).

-spec disconnect(pid()) -> ok | {error, term()}.
disconnect(_Pid) ->
    {error, not_implemented}.


-spec stop(pid()) -> ok.
stop(Pid) ->
    call_infinity(Pid, stop).

%% @doc Starts replication using existing connection from the specifyed position in the
%% log. When `require_ack` options is provided the socket would expect periodic
%% confirmations that N amount of events have been processed before sending more
%% messages to the client. This is the simple measure to protect the client code
%% from overloading.
-spec start_replication(pid(), [require_ack
                               | {offset, wal_offset() | 0 | eof}
                               ]) ->
          ok | {error, term()}.
start_replication(Pid, Opts) ->
    call_request(Pid, {start_replication, Opts}).

%% @doc Asynchronous confirmation from the client side, that N amount of
%% received messages have been processed. This call could be used together with
%% a `require_ack` option to make sure wal events do not overload the calling
%% process.
-spec get_next_stream_bulk(pid(), non_neg_integer()) -> ok | {error, term()}.
get_next_stream_bulk(Pid, N) ->
    gen_server:cast(Pid, {get_next_bulk, N}).

%% @doc Stops replication from the server.
-spec stop_replication(pid()) -> ok | {error, term()}.
stop_replication(Pid) ->
    call_request(Pid, {stop_replication}).

call_request(Pid, Msg) ->
    call_infinity(Pid, {request, Msg}).

call_infinity(Pid, Msg) ->
    gen_server:call(Pid, Msg, infinity).


%%------------------------------------------------------------------------------

init([Address, Port, Options, CallingProcess]) ->
    Backoff0 = backoff:init(?RECONN_MIN, ?RECONN_MAX, self(), reconnect),
    Backoff1 = backoff:type(Backoff0, jitter),

    _ = erlang:monitor(process, CallingProcess),
    State = #state{ address = Address,
                    port = Port,
                    opts = Options,
                    reconnect_backoff = Backoff1,
                    owner = CallingProcess
                  },

    case connect_int(Address, Port, Options, State) of
        {ok, State1} ->
            {ok, State1};
        {error, Reason} ->
            case maybe_reconnect(Reason, State) of
                {noreply, State1} ->
                    {ok, State1};
                Ret ->
                    Ret
            end
    end.

handle_call({request, Msg}, {Pid, _} = ClRef, #state{owner = Pid} = State) ->
    case State#state.socket_status of
        connected ->
            handle_client_msg(Msg, ClRef, State);
        disconnected ->
            {reply, {error, disconnected}, State}
    end;
handle_call(stop, _From, State) ->
    {stop, normal, ok, disconnect_int(State)};
handle_call(Msg, _, State) ->
    logger:warning("unhandled call msg: ~p~n", [Msg]),
    {reply, {error, not_implemented}, State}.

handle_cast({get_next_bulk, _N}, State) ->
    {noreply, State};
handle_cast(Msg, State) ->
    logger:warning("unhandled cast msg: ~p~n", [Msg]),
    {noreply, State}.

handle_info({tcp, Socket, Binary}, State) ->
    try
        ok = inet:setopts(Socket, [{active, once}]),
        handle_vx_wal_protocol(Binary, State)
    of
        {ok, StreamMsg} ->
            logger:debug("tcp_data: ~p~n", [StreamMsg]),
            State#state.owner ! #vx_client_msg{pid = self(),
                                               msg = StreamMsg
                                              },
            {noreply, State};
        {ok, ServerMsg, State1} ->
            {noreply, handle_server_msg(ServerMsg, State1)}
    catch _:_ ->
            logger:error("failed to parse data from server ~p~n", [Binary]),
            {stop, badproto, State}
    end;
handle_info({tcp_error, _, Reason}, State) ->
    maybe_reconnect(Reason, State);
handle_info({tcp_closed, _}, State) ->
    maybe_reconnect(tcp_closed, State);
handle_info({timeout, Tref, reconnect}, #state{reconnect_tref = Tref} = State) ->
    Backoff = State#state.reconnect_backoff,
    case
        connect_int(State#state.address, State#state.port,
                     State#state.opts, State)
    of
        {ok, State1} ->
            {noreply, State1#state{reconnect_backoff = backoff:succeed(Backoff),
                                   reconnect_tref = undefined
                                  }};
        {error, Reason} ->
            maybe_reconnect(Reason, State)
    end;
handle_info({'EXIT', _, process, Pid, Reason}, State = #state{owner = Pid}) ->
    %% Controling process terminated, do the cleanup.
    {stop, Reason, disconnect_int(State)};
handle_info(Msg, State) ->
    logger:warning("unhandled cast msg: ~p~n", [Msg]),
    {noreply, State}.

terminate(_Reason, _State) -> ok.

%%------------------------------------------------------------------------------

handle_vx_wal_protocol(Binary, State = #state{last_req_id = Ref}) ->
    Data = binary_to_term(Binary),
    case Data of
        #vx_wal_txn{} ->
            {ok, Data};
        #vx_srv_res{ref = R, msg = Msg} when R == Ref ->
            {ok, Msg, State#state{last_req_id = undefined}};
        _ ->
            throw({unexpected_msg, Data})
    end.

handle_server_msg(#vx_srv_start_res{rep_id = RepId}, State) ->
    gen_server:reply(State#state.owner_req, ok),
    State#state{wal_replication = RepId};
handle_server_msg(#vx_srv_stop_res{}, State) ->
    gen_server:reply(State#state.owner_req, ok),
    State#state{wal_replication = undefined};
handle_server_msg({error, Term}, State) ->
    %% We expect Term here to be an atom, that describes a server error
    gen_server:reply(State#state.owner_req, {error, Term}).

handle_client_msg({start_replication, Opts}, CliRef, State) ->
    RequireAck = proplists:get_bool(require_ack, Opts),
    Offset = proplists:get_value(offset, Opts, none),
    case RequireAck of
        true ->
            {reply, {error, {require_ack,is_not_supported}}, State};
        false ->
            case State#state.wal_replication of
                undefined ->
                    send_request(
                      #vx_cli_req{msg =
                                      #vx_cli_start_req{opts = [{offset, Offset}]}
                                 }, CliRef, State);
                RepId ->
                    {reply, {error, {already_started, RepId}}, State}
            end
    end;
handle_client_msg({stop_replication}, CliRef, State) ->
    case State#state.wal_replication of
        undefined ->
            {reply, {error, no_active_replication}, State};
        requested ->
            {reply, {error, no_active_replication}, State};
        RepId ->
            send_request(
              #vx_cli_req{msg = #vx_cli_stop_req{rep_id = RepId}}, CliRef, State)
    end.

send_request(Request, CliRef, State) ->
    case gen_tcp:send(State#state.socket, term_to_binary(Request)) of
        ok ->
            {noreply, State#state{last_req_id = Request#vx_cli_req.ref,
                                  owner_req = CliRef
                                 }};
        {error, Reason} = Error ->
            %% No support for restarting query after failure for now
            gen_server:reply(CliRef, {error, Reason}),
            case maybe_reconnect(Reason, State) of
                {noreply, State1} ->
                    {reply, Error, State1};
                {stop, _, State1} ->
                    {stop, Error, State1}
            end
    end.

maybe_reconnect(Error, State = #state{reconnect_backoff = Backoff0, opts = Options}) ->
    catch gen_tcp:close(State#state.socket),
    case proplists:get_bool(auto_reconnect, Options) of
        true ->
            {_, Backoff1} = backoff:fail(Backoff0),
            {noreply, schedule_reconnect(State#state{reconnect_backoff = Backoff1,
                                                     socket = undefined,
                                                     socket_status = disconnected
                                                    })};
        false ->
            {stop, {shutdown, Error}, State}
    end.

connect_int(Address, Port, Options, State) ->
    ConnectionTimeout = proplists:get_value(connection_timeout, Options,
                                           State#state.connect_timeout),
    KeepAlive = proplists:get_value(keepalive, Options, true),
    case gen_tcp:connect(Address, Port, [{packet, 4}, binary, {active, once},
                                         {keepalive, KeepAlive}
                                        ], ConnectionTimeout)
    of
        {ok, Socket} ->
            {ok, State#state{socket = Socket,
                             socket_status = connected
                            }};
        {error, _} = Error ->
            Error
    end.

disconnect_int(State = #state{socket = Socket}) ->
    catch gen_tcp:close(Socket),
    State#state{socket = undefined}.

schedule_reconnect(State = #state{reconnect_backoff = Backoff}) ->
    State#state{reconnect_tref = backoff:fire(Backoff)}.
