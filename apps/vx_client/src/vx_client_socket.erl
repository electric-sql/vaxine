%%%-------------------------------------------------------------------
%%% @doc Client socket
%%% @end
%%%-------------------------------------------------------------------
-module(vx_client_socket).
-behaviour(gen_statem).

%% API
-export([start_link/3,
         get_next_stream_bulk/2,
         start_replication/2,
         stop_replication/1,
         stop/1
        ]).

%% gen_statem callbacks
-export([callback_mode/0,
         init/1,
         terminate/3
        ]).

-export([ connected/3,
          disconnected/3,
          limited_streaming/3,
          streaming/3,
          wait_stop_stream/3
        ]).

-define(CONN_TIMEOUT, 1000).
-define(RECONN_MIN, 500).
-define(RECONN_MAX, 5000). %% 5 seconds

-include("vx_proto.hrl").

-type address() :: inet:socket_address() | inet:hostname().

-record(start_replication, {opts :: list()}).
-record(stop_replication, {}).
-record(get_next_bulk, {n :: non_neg_integer()}).

-record(state, { address :: address(),
                 port :: inet:port_number(),
                 opts :: [ gen_tcp:connect_option()
                        % | inet:inet_backend()
                         ],
                 socket :: gen_tcp:socket() | undefined,
                 connect_timeout = ?CONN_TIMEOUT :: non_neg_integer(),
                 reconnect_backoff = backoff:backoff(),
                 reconnect_tref :: reference() | undefined,
                 wal_replication :: reference() | undefined,
                 owner :: pid(),
                 last_req_id :: reference() | undefined,
                 owner_req :: term() | undefined,
                 sync_mode :: false | non_neg_integer()
               }).

-spec start_link(address(), inet:port_number(), list()) ->
          {ok, pid()} | ignore | {error, term()}.
start_link(Address, Port, Options) ->
    gen_statem:start_link(?MODULE, [Address, Port, Options, self()], []).

-spec stop(pid()) -> ok | {error, term()}.
stop(Pid) ->
    call_infinity(Pid, stop).

%% @doc Starts replication using existing connection from the specifyed position
%% in the log. When `sync` options is provided the socket would expect periodic
%% confirmations that N amount of events have been processed before sending more
%% messages to the client. This is the simple measure to protect the client code
%% from overloading.
-spec start_replication(pid(), [ {sync, non_neg_integer()}
                               | {offset, wal_offset() | 0 | eof}
                               ]) ->
          ok | {error, term()}.
start_replication(Pid, Opts) ->
    call_request(Pid, #start_replication{opts = Opts}).

%% @doc Asynchronous confirmation from the client side, that N amount of
%% received messages have been processed. This call could be used together with
%% a `sync` option to make sure wal events do not overload the calling process.
-spec get_next_stream_bulk(pid(), non_neg_integer()) -> ok | {error, term()}.
get_next_stream_bulk(Pid, N) ->
    gen_server:call(Pid, #get_next_bulk{n = N}).

%% @doc Stops replication from the server.
-spec stop_replication(pid()) -> ok | {error, term()}.
stop_replication(Pid) ->
    call_request(Pid, #stop_replication{}).

call_request(Pid, Msg) ->
    call_infinity(Pid, Msg).

call_infinity(Pid, Msg) ->
    gen_server:call(Pid, Msg, infinity).

%%------------------------------------------------------------------------------
%% gen_server callbacks
%%------------------------------------------------------------------------------

callback_mode() ->
    state_functions.

init([Address, Port, Options, CallingProcess]) ->
    process_flag(trap_exit, true),

    Backoff0 = backoff:init(?RECONN_MIN, ?RECONN_MAX, self(), reconnect),
    Backoff1 = backoff:type(Backoff0, jitter),

    _ = erlang:monitor(process, CallingProcess),
    State = #state{ address = Address,
                    port = Port,
                    opts = Options,
                    reconnect_backoff = Backoff1,
                    owner = CallingProcess,
                    sync_mode = false
                  },

    case connect_int(Address, Port, Options, State) of
        {ok, State1} ->
            {ok, connected, State1};
        {error, Reason} ->
            case maybe_reconnect(Reason, State) of
                {next_state, disconnected = Phase, State1} ->
                    {ok, Phase, State1};
                Ret ->
                    Ret
            end
    end.

%% In connected mode process operate in request/response manner.
%% We support single request at a time.
connected({call, CliRef}, #start_replication{} = Msg, State) ->
    handle_client_msg(Msg, ?FUNCTION_NAME, CliRef, State);
connected({call, CliRef}, #stop_replication{}, State) ->
    {keep_state, State, [{reply, CliRef, {error, no_active_replication}}]};
connected({call, CliRef}, #get_next_bulk{}, State) ->
    {keep_state, State, [{reply, CliRef, {error, no_active_replication}}]};
connected(info, {tcp, Socket, Binary}, #state{socket = Socket} = State) ->
    try handle_vx_wal_protocol(Binary, State) of
        {ok, ServerMsg, State1} ->
            handle_server_msg(ServerMsg, State1#state.owner_req,
                              State1#state{owner_req = undefined}
                             )
    catch T:E ->
            logger:error("failed to parse data from server ~p~n", [Binary]),
            maybe_reconnect({T,E}, State)
    end;
connected(info, {tcp_error, Socket, Reason}, #state{socket = Socket} = State) ->
    maybe_reconnect(Reason, State);
connected(info, {tcp_closed, Socket}, #state{ socket = Socket } = State) ->
    maybe_reconnect(tcp_closed, State);
connected(EventType, EventContent, Data) ->
    default_handling(EventType, EventContent, Data).

%% In streaming mode messages are send to the client immediatly and no
%% acking from the client is required
streaming({call, CliRef}, #start_replication{}, State) ->
    {keep_state, State, [{reply, CliRef, {error, already_started}}]};
streaming({call, CliRef}, #stop_replication{} = Msg, State) ->
    handle_client_msg(Msg, ?FUNCTION_NAME, CliRef, State);
streaming({call, CliRef}, #get_next_bulk{}, State) ->
    {keep_state, State, [{reply, CliRef, {error, wrong_mode}}]};
streaming(info, {tcp, Socket, Binary}, #state{socket = Socket} = State) ->
    try
        ok = inet:setopts(Socket, [{active, once}]),
        handle_vx_wal_protocol(Binary, State)
    of
        {ok, StreamMsg} ->
            State#state.owner ! #vx_client_msg{pid = self(),
                                               msg = StreamMsg
                                              },
            {keep_state, State}
    catch T:E ->
            %% FIXME: If we intend to support auto-reconnect after such a failure
            %% we need to notify the client somehow.
            logger:error("failed to parse data from server ~p~n", [Binary]),
            {stop, {T,E}}
    end;
streaming(info, {tcp_error, _, Reason}, _State) ->
    %% FIXME: If we intend to support auto-reconnect after such a failure
    %% we need to notify the client somehow.
    logger:error("tcp error ~p terminate the process ~n", [Reason]),
    {stop, {tcp_error, Reason}};
streaming(info, {tcp_closed, _}, _State) ->
    {stop, tcp_closed};
streaming(EventType, EventContent, Data) ->
    default_handling(EventType, EventContent, Data).

limited_streaming({call, CliRef}, #start_replication{}, State) ->
    {keep_state, State, [{reply, CliRef, {error, already_started}}]};
limited_streaming({call, CliRef}, #stop_replication{} = Msg, State) ->
    handle_client_msg(Msg, ?FUNCTION_NAME, CliRef, State);
limited_streaming({call, CliRef}, #get_next_bulk{n = N},
                  #state{sync_mode = Mode, socket = Socket} = State) ->
    ok = inet:setopts(Socket, [{active, N}]),
    {keep_state, State#state{sync_mode = N + Mode},
     [{reply, CliRef, ok}]
    };
limited_streaming(info, {tcp, Socket, Binary},
                  #state{socket = Socket, sync_mode = Remaining} = State) ->
    try
        handle_vx_wal_protocol(Binary, State)
    of
        {ok, StreamMsg} ->
            State#state.owner !
                #vx_client_msg{pid = self(),
                               msg = StreamMsg,
                               await_sync = (Remaining =< 1)
                              },
            {keep_state, State#state{sync_mode = Remaining - 1}}
    catch T:E ->
            %% FIXME: If we intend to support auto-reconnect after such a failure
            %% we need to notify the client somehow.
            logger:error("failed to parse data from server ~p~n", [Binary]),
            {stop, {T,E}}
    end;
limited_streaming(info, {tcp_error, _, Reason}, _State) ->
    %% FIXME: If we intend to support auto-reconnect after such a failure
    %% we need to notify the client somehow.
    logger:error("tcp error ~p terminate the process ~n", [Reason]),
    {stop, {tcp_error, Reason}};
limited_streaming(info, {tcp_closed, _}, _State) ->
    {stop, tcp_closed};
limited_streaming(EventType, EventContent, Data) ->
    default_handling(EventType, EventContent, Data).

wait_stop_stream(info, {tcp, Socket, Binary}, #state{socket = Socket} = State) ->
    ok = inet:setopts(Socket, [{active, once}]),
    try handle_vx_wal_protocol(Binary, State) of
        {ok, _} ->
            {keep_state, State};
        {ok, #vx_srv_stop_res{} = Msg, State1} ->
            handle_server_msg(Msg, State1#state.owner_req,
                              State1#state{owner_req = undefined})
    catch T:E ->
          logger:error("failed to parse data from server ~p~n", [Binary]),
            {stop, {T,E}}
    end;
wait_stop_stream(info, {tcp_error, _, Reason}, _State) ->
    %% FIXME: If we intend to support auto-reconnect after such a failure
    %% we need to notify the client somehow.
    logger:error("tcp error ~p terminate the process ~n", [Reason]),
    {stop, {tcp_error, Reason}};
wait_stop_stream(info, {tcp_closed, _}, _State) ->
    {stop, tcp_closed};
wait_stop_stream(EventType, EventContent, Data) ->
    default_handling(EventType, EventContent, Data).

disconnected({call, CliRef}, #start_replication{}, State) ->
    {keep_state, State, [{reply, CliRef, {error, not_connected}}]};
disconnected({call, CliRef}, #stop_replication{}, State) ->
    {keep_state, State, [{reply, CliRef, {error, not_connected}}]};
disconnected({call, CliRef}, #get_next_bulk{}, State) ->
    {keep_state, State, [{reply, CliRef, {error, not_connected}}]};
disconnected(info, {tcp_error, _, Reason}, _State) ->
    %% That's unexpected in this state, terminate even
    logger:error("unexpected tcp error ~p terminate the process ~n", [Reason]),
    {stop, {tcp_error, Reason}};
disconnected(info, {tcp_closed, _}, _State) ->
    logger:error("unexpected tcp close, terminate the process ~n", []),
    {stop, tcp_closed};
disconnected(EventType, EventContent, Data) ->
    default_handling(EventType, EventContent, Data).

terminate(_Reason, _State, _Data) ->
    ok.

%%------------------------------------------------------------------------------
%% Private callbacks
%%------------------------------------------------------------------------------

connect_int(Address, Port, Options, State) ->
    ConnectionTimeout = proplists:get_value(connection_timeout, Options,
                                           State#state.connect_timeout),
    KeepAlive = proplists:get_value(keepalive, Options, true),
    case gen_tcp:connect(Address, Port, [{packet, 4}, binary,
                                         {active, once},
                                         {keepalive, KeepAlive}
                                        ], ConnectionTimeout)
    of
        {ok, Socket} ->
            {ok, State#state{socket = Socket}};
        {error, _} = Error ->
            Error
    end.

maybe_reconnect(Error, State = #state{reconnect_backoff = Backoff0,
                                      opts = Options}) ->
    case State#state.owner_req of
        undefined -> ok;
        From      ->
            gen_statem:reply(From, {error, disconnected})
    end,
    catch gen_tcp:close(State#state.socket),
    case proplists:get_bool(auto_reconnect, Options) of
        true ->
            {_, Backoff1} = backoff:fail(Backoff0),
            {next_state, disconnected,
             schedule_reconnect(State#state{reconnect_backoff = Backoff1,
                                            socket = undefined,
                                            owner_req = undefined
                                           })};
        false ->
            {stop, {shutdown, Error}, State}
    end.

%% disconnect_int(State = #state{socket = Socket}) ->
%%     catch gen_tcp:close(Socket),
%%     State#state{socket = undefined}.

handle_server_msg(#vx_srv_start_res{rep_id = RepId}, CliRef, State) ->
    Reply = [{reply, CliRef, ok}],
    case State#state.sync_mode of
        false ->
            ok = inet:setopts(State#state.socket, [{active, once}]),

            {next_state, streaming,
             State#state{wal_replication = RepId}, Reply};
        N ->
            ok = inet:setopts(State#state.socket, [{active, N}]),

            {next_state, limited_streaming,
             State#state{wal_replication = RepId}, Reply}
    end;
handle_server_msg(#vx_srv_stop_res{}, CliRef, State) ->
    {next_state, connected, State#state{wal_replication = undefined},
     [{reply, CliRef, ok}]};
handle_server_msg({error, Term}, CliRef, State) ->
    %% We expect Term here to be an atom, that describes a server error
    {next_state, connected, State#state{wal_replication = undefined},
     [{repy, CliRef, {error, Term}}]}.

handle_client_msg(_Msg, _, CliRef, State = #state{last_req_id = ReqId})
  when ReqId =/= undefined ->
    {keep_state, State,
     [{reply, CliRef, {error, multiple_requests_not_supported}}]
    };
handle_client_msg(#start_replication{opts = Opts}, _, CliRef, State) ->
    case State#state.wal_replication of
        undefined ->
            RequireAck = proplists:get_value(sync, Opts, false),
            Offset     = proplists:get_value(offset, Opts, none),
            State1     = State#state{sync_mode = RequireAck},

            send_request(
              #vx_cli_req{msg =
                              #vx_cli_start_req{opts = [{offset, Offset}]}
                         }, CliRef, State1);
        _RepId ->
            {keep_state, State, [{reply, CliRef, {error, already_established}}]}
    end;
handle_client_msg(#stop_replication{}, _, CliRef, State) ->
    case State#state.wal_replication of
        undefined ->
            {keep_state, State,
             [{reply, CliRef, {error, no_active_replication}}]
            };
        RepId ->
            ok = inet:setopts(State#state.socket, [{active, once}]),
            case send_request(#vx_cli_req{msg = #vx_cli_stop_req{rep_id = RepId}},
                              CliRef, State)
            of
                {keep_state, State1} ->
                    {next_state, wait_stop_stream, State1};
                Etc ->
                    Etc
            end
    end.

handle_vx_wal_protocol(Binary, State = #state{last_req_id = Ref}) ->
    Data = binary_to_term(Binary),
    case Data of
        #vx_wal_txn{} ->
            logger:debug("received #vx_wal_txn from server: ~p - ~p~n",
                         [Data#vx_wal_txn.txid, Data#vx_wal_txn.wal_offset]),
            {ok, Data};
        #vx_srv_res{ref = R, msg = Msg} when R == Ref ->
            logger:debug("received #vx_srv_res from server: ~p~n",[Data]),
            {ok, Msg, State#state{last_req_id = undefined}};
        _ ->
            logger:warning("Unexpected data received: ~p~n", [ Data ]),
            throw({unexpected_msg, Data})
    end.

send_request(Request, CliRef, State) ->
    case gen_tcp:send(State#state.socket, term_to_binary(Request)) of
        ok ->
            {keep_state, State#state{last_req_id = Request#vx_cli_req.ref,
                                     owner_req = CliRef
                                    }};
        {error, Reason} ->
            gen_statem:reply(CliRef, {error, Reason}),
            %% No support for restarting query after failure for now
            maybe_reconnect(Reason, State)
    end.

schedule_reconnect(State = #state{reconnect_backoff = Backoff}) ->
    State#state{reconnect_tref = backoff:fire(Backoff)}.

default_handling(info, {'EXIT', Owner, Reason}, #state{owner = Owner}) ->
    logger:debug("Owner of the socket process terminated ~p~n", [Owner]),
    {stop, {shutdown, Reason}};
default_handling(info, {tcp_passive, _}, State) ->
    {keep_state, State};
default_handling({call, CliReq}, stop, #state{socket = Socket}) ->
    case Socket of
        undefined ->
            ok;
        _ ->
            gen_tcp:close(Socket)
    end,
    {stop_and_reply, normal, [{reply, CliReq, ok}]};
default_handling({call, CliReq}, EventContent, Data) ->
    logger:notice("Unsupported call ~p~n", [EventContent]),
    {keep_state, Data, [{reply, CliReq, {error, unhandled_msg}}]};
default_handling(EventType, EventContent, Data) ->
    logger:notice("Unexpected and unhandled ~p ~p~n",
                 [EventType, EventContent]),
    {keep_state, Data}.
