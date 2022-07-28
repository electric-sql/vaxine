%% @doc Nofitication service for logging events. Handler are supposed to be
%% light-weight and should not spend too much time in M:F/2 calls.

-module(logging_notification_server).
-behaviour(gen_event).

-export([ start_link/0,
          add_handler/3,
          delete_handler/1,
          notify_commit/4,
          stop/0
        ]).
-export([ init/1,
          handle_event/2,
          handle_call/2,
          handle_info/2,
          terminate/2
        ]).

-record( state, { handler :: handler() } ).
-type state() :: #state{}.
-type handler() :: {module(), atom(), term()}.

start_link() ->
    gen_event:start_link({local, ?MODULE}, []).

stop() ->
    gen_event:stop(?MODULE).

%% @doc Add subscribers handler. Handler should be as light-weight as possible,
%% as it affects the flow of committed transactions.
-spec add_handler(module(), atom(), term()) -> ok.
add_handler(M, F, HandlerState) ->
    gen_event:add_sup_handler(?MODULE, {?MODULE, self()}, {M, F, HandlerState}).

delete_handler(Args) ->
    gen_event:delete_handler(?MODULE, {?MODULE, self()}, Args).

%% @doc Notify subscribers about new committed txn on the specific partition.
-spec notify_commit(antidote:partition_id(), antidote:txid(),
                    {antidote:dcid(), antidote:clock_time()},
                    antidote:snapshot_time()) ->
          ok.
notify_commit(Partition, TxId, CommitTime, SnapshotTime) ->
    gen_event:sync_notify(?MODULE,
                          {commit, [Partition, TxId, CommitTime, SnapshotTime]}).

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

init({M, F, A}) ->
    {ok, #state{ handler = {M, F, A} }}.

handle_call(Msg, State) ->
    State1 = apply_handler(Msg, State),
    {ok, _Reply = ok, State1}.

handle_event({commit, Msg}, State) ->
    try
        State1 = apply_handler(Msg, State),
        {ok, State1}
    catch T:E:S ->
            logger:error("Handler crashed: ~p:~p Stack: ~p~n", [T, E, S]),
            remove_handler
    end.

handle_info(Msg, State) ->
    logger:warning("Unexpected info message: ~p~n", [Msg]),
    {ok, State}.

terminate(_Arg, _State) ->
    ok.

-spec apply_handler(term(), state()) -> state().
apply_handler(Info, State = #state{handler = {M, F, HandlerState0}}) ->
    case apply(M, F, HandlerState0 ++ Info) of
        ok ->
            State;
        {ok, HandlerState1} ->
            State#state{handler = {M, F, HandlerState1}}
    end.
