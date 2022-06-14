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

-include_lib("kernel/include/logger.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type partition() :: integer().
-type sn() :: antidote:snapshot_time().
-type key() :: binary().
-type sub_id() :: reference().
-export_type([sub_id/0]).

-record(keys_notif, { keys :: [ key() ],
                       snapshot :: sn(),
                       partition :: partition()
                     }).

-record(ping_notif, { snapshot :: sn(),
                      partition :: partition()
                    }).

-record(state, { clients = [] ::
                   [{WorkerPid :: pid(),
                     { Port   :: port(), MonRef :: reference(), Subs   :: [sub_id()] }
                    }],
                 subs = [] :: [{SubId :: sub_id(),
                                { [key()], sn(), pid() }
                               }],
                 parts = #{} :: #{partition() => sn()},
                 proxies = #{} :: #{ pid() => [ partition() ],
                                     partition() => pid()
                                   },
                 %% Ordered list of snapshots with sub ids (causaly increasing order)
                 sn2subs = vector_orddict:new() :: vector_orddict:vector_orddict(),
                 %% Keys to sub_id matching
                 keys2subs = #{} :: #{ key() => [sub_id()] }
               }).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc API for vx_subs_workers
-spec notify_new_client(pid(), port()) -> ok.
notify_new_client(Pid, Socket) ->
    gen_server:cast(?MODULE, {new_client, Pid, Socket}).

-spec subscribe([binary()], antidote:snapshot_time()) ->
          {ok, sub_id()} | {error, term()}.
subscribe(Keys, Snapshot) when is_list(Keys) ->
    gen_server:call(?MODULE, {subscribe, Keys, Snapshot}, infinity).

-spec unsubscribe(sub_id()) -> ok | {error, term()}.
unsubscribe(SubId) ->
    gen_server:cast(?MODULE, {unsubscribe, SubId}).

%%------------------------------------------------------------------------------

init(_) ->
    {ok, #state{} }.

handle_call({subscribe, Keys, Snapshot} = Msg, {_, Pid}, State) ->
    ?LOG_DEBUG("subscribe request ~p from ~p~n", [Msg, Pid]),
    case is_active_client(Pid, State) of
        true ->
            {SubId, State1} = add_sub_int(Pid, Keys, Snapshot, State),
            {reply, {ok, SubId}, State1};
        false ->
            {reply, {error, unknown_client}, State}
    end;
handle_call({unsubscribe, SubId} = Msg, {_, Pid}, State) ->
    ?LOG_DEBUG("unsubscribe request ~p from ~p~n", [Msg, Pid]),
    case remove_sub_int(SubId, State) of
        {false, State1} ->
            {reply, {error, {unknown_sub, SubId}}, State1};
        {true, State1} ->
            {reply, ok, State1}
    end;
handle_call({status}, _, State) ->
    %% FIXME: Add status info for debug purposes
    {reply, ok, State};
handle_call(Msg, {_, Pid}, State) ->
    ?LOG_WARNING("unknown hand_call ~p from ~p~n", [Msg, Pid]),
    {reply, {error, not_implemented}, State}.

handle_cast({new_client, Pid, Socket}, State) ->
    State1 = add_client_int(Pid, Socket, State),
    {noreply, State1};
handle_cast(Msg, State) ->
    ?LOG_WARNING("unknown hand_cast ~p~n", [Msg]),
    {norpely, State}.

handle_info({'EXIT', _MonRef, process, Pid, _}, State) ->
    State1 = cleanup_client_int(Pid, State),
    {noreply, State1};

handle_info(#keys_notif{keys = Keys, snapshot = Sn, partition = Partition} = E, State0) ->
    ?LOG_DEBUG("key notification ~p~n", [E]),
    {PSn, State1} = update_partial_snapshot(Sn, Partition, State0),

    SubID2Keys = get_subids_map(Sn, Keys, State0),
    maybe_keys_notifications(SubID2Keys, State0),

    State2 = add_snapshot_to_queue(Sn, maps:keys(SubID2Keys), State1),
    maybe_snapshot_notifications(PSn, State2),

    {noreply, State2};
handle_info(#ping_notif{snapshot = Sn, partition = Partition} = E, State0) ->
    ?LOG_DEBUG("ping notification ~p~n", [E]),
    {PSn, State1} = update_partial_snapshot(Sn, Partition, State0),
    maybe_snapshot_notifications(PSn, State1),
    {noreply, State1};

handle_info(Msg, State) ->
    ?LOG_WARNING("unknown handle_info ~p from ~p~n", [Msg]),
    {norpely, State}.

terminate(_, _) ->
    ok.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

add_client_int(Pid, Socket, State = #state{clients = Clients}) ->
    MonRef = erlang:monitor(process, Pid),
    Clients1 = lists:keystore(Pid, 1, Clients, {Socket, MonRef, []}),
    State#state{clients = Clients1}.

is_active_client(Pid, State) ->
    case lists:keyfind(Pid, 1, State#state.clients) of
        false ->
            false;
        {P, _} when P==Pid ->
            true
    end.

cleanup_client_int(Pid, State = #state{clients = Clients}) ->
    case lists:keytake(Pid, 1, Clients) of
        {value, {_Socket, MonRef}, Clients1} ->
            catch erlang:demonitor(MonRef),

            State#state{clients = Clients1
                       };
        false ->
            State
    end.

%% Invariant - this call expects client to be in the state
add_sub_int(Pid, Keys, Snapshot, State) ->
    SubId = make_ref(),

    _ClientInfo = {Pid, {P, M, S}} = lists:keyfind(Pid, 1, State#state.clients),
    State1 = State#state{
               subs = [{ SubId, {Keys, Snapshot, Pid} } | State#state.subs ],
               clients = lists:keyreplace(Pid, 1, State#state.clients,
                                          {P, M, [SubId | S ]}
                                         )
              },
    {SubId, State1}.

remove_sub_int(SubId, State) ->
    case lists:keytake(SubId, 1, State#state.subs) of
        false ->
            {false, State};
        {value, _, Subs} ->
            {true, State#state{subs = Subs}}
    end.

get_subids_map(Sn, Keys, #state{keys2subs = T, subs = Subs}) ->
    %% lists:usort(
    %%  maps:values(maps:with(Keys, T))).
    FilterSubs = maps:with(Keys, T),
    %% #{ SubId => [ Key ] }
    SubID2keys =
        maps:fold(
          fun(K, SubIds, Acc) ->
                  lists:foldl(
                    fun(Sub, Acc0) ->
                            case Acc0 of
                                #{Sub := V} -> Acc0#{Sub := [K|V]};
                                #{}         -> Acc0#{Sub => [K]}
                            end
                    end, Acc, SubIds)
          end, #{}, FilterSubs),
    maps:filter(fun(SubId, _) ->
                        {SubId, {_, SnSub, _}} = lists:keyfind(SubId, 1, Subs),
                        vectorclock:ge(Sn, SnSub)
                end, SubID2keys).

update_partial_snapshot(Sn, Pt, #state{parts = Parts0} = State) ->
    Parts1 = Parts0#{ Pt => Sn },
    {merge(Parts0), State#state{parts = Parts1}}.

add_snapshot_to_queue(Sn, SubIds, #state{sn2subs = SnOrdered0} = State) ->
    SnOrderedList = vector_orddict:to_list(SnOrdered0),
    SnOrdered1 =
        case lists:keyfind(Sn, 1, SnOrderedList) of
            false ->
                vector_orddict:insert(Sn, SubIds, SnOrdered0);
            {Sn, SL} ->
                SnOrderedList1 =
                    lists:keyreplace(Sn, 1, SnOrderedList, {Sn, lists:usort(SL ++ SubIds)}),
                vector_orddict:from_list(SnOrderedList1)
        end,
    State#state{ sn2subs = SnOrdered1 }.

maybe_keys_notifications(SubID2Keys, #state{} = State) ->
    ok.

maybe_snapshot_notifications(PSn, #state{ sn2subs = SnOrdered0 } = State) ->
    ReverseOrder = lists:reverse( vector_orddict:to_list(SnOrdered0) ),
    SnGreaterList =
        lists:dropwhile(
          fun({Sn, SubIds}) ->
                  case vectorclock:ge(PSn, Sn) of
                      true ->
                          send_snapshot_notification(Sn, SubIds, State),
                          true;
                      false ->
                          false
                  end
          end, ReverseOrder),
    vector_orddict:from_list(SnGreaterList).

send_snapshot_notification(SN, SubIds, State) ->
    ok.

%% FIXME: Probably all_defined should be called on proxies only
merge(VcMap) ->
    case all_defined(VcMap) of
        true ->
            vectorclock:min(maps:values(VcMap));
        false ->
            ?LOG_ERROR("vectorclock with missing entries: ~p", [VcMap]),
            %% FIXME:
            erlang:error({malformed_vv, VcMap})
    end.

all_defined(VcMap) ->
    maps:fold(fun(_K, V, Acc) -> Acc andalso V =/= undefined end, true, VcMap).
