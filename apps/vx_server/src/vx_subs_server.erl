-module(vx_subs_server).
-behaviour(gen_server).

-export([ start_link/0 ]).
-export([ init/1,
          handle_call/3,
          handle_cast/2,
          handle_info/2,
          terminate/2
        ]).

-export([ subscribe/2,
          unsubscribe/1,
          notify_new_client/2
        ]).

-type sub_id() :: reference().
-export_type([sub_id/0]).

-record(state, { clients = [] :: [{pid(), {port(), reference()}}]

               }).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc API for vx_subs_workers
-spec notify_new_client(pid(), port()) -> ok.
notify_new_client(Pid, Socket) ->
    gen_server:cast(?MODULE, {new_client, Pid, Socket}).

-spec subscribe([binary()], antidote:snapshot_time()) -> {ok, sub_id()} | {error, term()}.
subscribe(Keys, Snapshot) ->
    gen_server:call(?MODULE, {subscribe, Keys, Snapshot}, infinity).

-spec unsubscribe(sub_id()) -> ok | {error, term()}.
unsubscribe(SubId) ->
    gen_server:cast(?MODULE, {unsubscribe, SubId}).

%%------------------------------------------------------------------------------

init(_) ->
    {ok, #state{}}.

handle_call({subscribe, _Keys, _Snapshot}, {_, _Pid}, State) ->

    {reply, {ok, make_ref()}, State}.

handle_cast({new_client, Pid, Socket}, State) ->
    State1 = add_client(Pid, Socket, State),
    {noreply, State1}.

handle_info({'EXIT', _MonRef, process, Pid, _}, State) ->
    State1 = cleanup_client(Pid, State),
    {noreply, State1};

handle_info(_, State) ->
    {noreply, State}.

terminate(_, _) ->
    ok.

add_client(Pid, Socket, State = #state{clients = Clients}) ->
    MonRef = erlang:monitor(process, Pid),
    Clients1 = lists:keystore(Pid, 1, Clients, {Socket, MonRef}),
    State#state{clients = Clients1}.

cleanup_client(Pid, State = #state{clients = Clients}) ->
    case lists:keytake(Pid, 1, Clients) of
        {value, {_Socket, MonRef}, Clients1} ->
            catch erlang:demonitor(MonRef),
            State#state{clients = Clients1};
        false ->
            State
    end.
