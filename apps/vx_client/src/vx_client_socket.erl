-module(vx_client_socket).
-behaviour(gen_server).

-export([start_link/2, start_link/3,
         disconnect/1, subscribe/4, unsubscribe/2,
         stop/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2]).

-define(CONN_TIMEOUT, 1000).
-define(RECONN_MIN, 500).
-define(RECONN_MAX, 5000). %% 5 seconds

-type address() :: inet:socket_address() | inet:hostname().
-type sub_id() :: vx_client:sub_id().

-record(state, { address :: address(),
                 port :: inet:port_number(),
                 opts :: [ gen_tcp:connect_option()
                        % | inet:inet_backend()
                         ],
                 socket :: gen_tcp:socket() | undefined,
                 connect_timeout = ?CONN_TIMEOUT :: non_neg_integer(),
                 reconnect_backoff = backoff:backoff(),
                 reconnect_tref :: reference() | undefined
               }).

-spec start_link(address(), inet:port_number()) ->
          {ok, pid()} | {error, term()}.
start_link(Address, Port) ->
    start_link(Address, Port, []).

-spec start_link(address(), inet:port_number(), list()) ->
          {ok, pid()} | {error, term()}.
start_link(Address, Port, Options) when is_list(Options) ->
    gen_server:start_link(?MODULE, [Address, Port, Options], []).

-spec disconnect(pid()) -> ok | {error, term()}.
disconnect(_Pid) ->
    %%gen_server:call(Pid, disconnect, infinity).
    {error, not_implemented}.

-spec subscribe(pid(), [binary()], term(), boolean()) ->
          {ok, sub_id()} | {error, term()}.
subscribe(_Pid, _Keys, _Snapshot, _SnapshotFlag) ->
    {error, not_implemented}.

-spec unsubscribe(pid(), sub_id()) -> ok | {error, term()}.
unsubscribe(_Pid, _SubId) ->
    {error, not_implemented}.

-spec stop(pid()) -> ok.
stop(Pid) ->
    gen_server:call(Pid, stop, infinity).

%%------------------------------------------------------------------------------

init([Address, Port, Options]) ->
    Backoff0 = backoff:init(?RECONN_MIN, ?RECONN_MAX, self(), reconnect),
    Backoff1 = backoff:type(Backoff0, jitter),

    State = #state{ address = Address,
                    port = Port,
                    opts = Options,
                    reconnect_backoff = Backoff1
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

handle_call(stop, _From, State) ->
    {stop, normal, ok, disconnect_int(State)};
handle_call(_, _, State) ->
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

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
handle_info(_, State) ->
    {noreply, State}.

terminate(_Reason, _State) -> ok.

%%------------------------------------------------------------------------------

maybe_reconnect(Error, State = #state{reconnect_backoff = Backoff0, opts = Options}) ->
    case proplists:get_bool(auto_reconnect, Options) of
        true ->
            {_, Backoff1} = backoff:fail(Backoff0),
            {noreply, schedule_reconnect(State#state{reconnect_backoff = Backoff1})};
        false ->
            {stop, Error}
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
            {ok, State#state{socket = Socket}};
        {error, _} = Error ->
            Error
    end.

disconnect_int(State = #state{socket = Socket}) ->
    catch gen_tcp:close(Socket),
    State#state{socket = undefined}.

schedule_reconnect(State = #state{reconnect_backoff = Backoff}) ->
    State#state{reconnect_tref = backoff:fire(Backoff)}.
