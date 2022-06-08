-module(vx_client_socket).
-behaviour(gen_server).

-export([start_link/2, start_link/3, stop/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2]).

-define(CONN_TIMEOUT, 1000).
-define(RECONN_INTERVAL, 1000).

-type address() :: inet:socket_address() | inet:hostname().

-record(state, { address :: address(),
                 port :: inet:port_number(),
                 opts :: [ gen_tcp:connect_option()
                         | inet:inet_backend()
                         ],
                 socket :: gen_tcp:socket() | undefined,
                 connect_timeout = ?CONN_TIMEOUT :: non_neg_integer(),
                 reconnect_interval = ?RECONN_INTERVAL :: non_neg_integer(),
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

-spec stop(pid()) -> ok.
stop(Pid) ->
    gen_server:call(Pid, stop, infinity).

%%------------------------------------------------------------------------------

init([Address, Port, Options]) ->
    State = #state{ address = Address,
                    port = Port,
                    opts = Options
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
    {noreply, disconnect_int(State)};
handle_call(_, _, State) ->
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({tcp_error, _, Reason}, State) ->
    maybe_reconnect(Reason, State);
handle_info({tcp_closed, _}, State) ->
    maybe_reconnect(tcp_closed, State);
handle_info({timeout, Tref, reconnect}, #state{reconnect_tref = Tref} = State) ->
    case
        connect_int(State#state.address, State#state.port,
                     State#state.opts, State)
    of
        {ok, State1} ->
            {noreply, State1};
        {error, Reason} ->
            maybe_reconnect(Reason, State)
    end;
handle_info(_, State) ->
    {noreply, State}.

terminate(_Reason, _State) -> ok.

%%------------------------------------------------------------------------------

maybe_reconnect(Error, State = #state{opts = Options}) ->
    case proplists:get_bool(auto_reconnect, Options) of
        true ->
            {noreply, schedule_reconnect(State)};
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

schedule_reconnect(State) ->
    Tref = erlang:start_timer(State#state.reconnect_interval, self(), reconnect),
    State#state{ reconnect_tref = Tref }.
