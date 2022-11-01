-module(vx_wal_stream_server).
-behaviour(gen_server).

-export([start_link/0,
         register/0,
         subscribe/0,
         unsubscribe/0
        ]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3
        ]).

-record(state, { ets :: ets:tid(),
                 workers :: [{ reference(), pid() }]
               }).

-include("vx_wal_stream.hrl").
-define(EVENT_MNG, vx_wal_stream_inotify).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec register() -> ok.
register() ->
    gen_server:call(?MODULE, {register}).

%% @doc Subscribe to events related to WAL directory
-spec subscribe() -> ok.
subscribe() ->
    fs:subscribe(?EVENT_MNG).

%% @doc Unsubscribe from events related to WAL directory
-spec unsubscribe() -> ok.
unsubscribe() ->
    gen_event:delete_handler(?EVENT_MNG,
                             {fs_event_bridge, self()}, [self()]).

init(_) ->
   T = ets:new(wal_replication_status,
               [set, public, named_table,
                {read_concurrency, true},
                {write_concurrency, true},
                {keypos, #wal_replication_status.key}
               ]),

    %% NOTE: fs does not support subscription to concrete file events through
    %% inotifywait, but it's not limitation of the tool, but rather current
    %% backend module. Also path should point to a directory rather than a regular
    %% file
    {ok, Path} = application:get_env(antidote, data_dir),
    {ok, _} = fs:start_link(?EVENT_MNG, Path),

    {ok, #state{ ets = T,
                 workers = []
               } }.

handle_call({register}, {Pid, _}, State) ->
    case lists:keyfind(Pid, 2, State#state.workers) of
        false ->
            MonRef = erlang:monitor(process, Pid),
            Workers = [{MonRef, Pid} | State#state.workers],

            {reply, ok, State#state{workers = Workers}};
        _ ->
            {reply, {error, already_registered}, State}
    end;

handle_call(_Info, _From, State) ->
    {reply, {error, not_implemented}, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info({'DOWN', MonRef, process, Pid, Reason}, State) ->
    Workers = lists:keydelete(MonRef, 1, State#state.workers),
    State1 = State#state{workers = Workers},
    case Reason of
        normal ->
            {noreply, State1};
        _ ->
            Partitions = dc_utilities:get_all_partitions(),
            _ = [
                 logging_notification_server:delete_handler(
                   {Partition, Pid})
                 || Partition <- Partitions
                ],
            {noreply, State1}
    end;
handle_info(_, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
