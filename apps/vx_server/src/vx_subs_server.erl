-module(vx_subs_server).
-behaviour(gen_server).

-export([ start_link/0 ]).
-export([ init/1,
          handle_call/3,
          handle_cast/2,
          handle_info/2,
          terminate/2
        ]).

-export([ subscribe/2, subscribe/1,
          unsubscribe/1,
          add_new_client/1,
          notify_keys/1,
          notify_pings/1
        ]).

-include_lib("kernel/include/logger.hrl").

-ifdef(EUNIT).
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

-type keys_notif() :: #keys_notif{}.
-type ping_notif() :: #ping_notif{}.

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
                 sn2subs :: vector_orddict:vector_orddict(),
                 %% Keys to sub_id matching
                 keys2subs = #{} :: #{ key() => [sub_id()] }
               }).

-type state() :: #state{}.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc API for vx_subs_workers
-spec add_new_client(port()) -> ok.
add_new_client(Socket) ->
    gen_server:cast(?MODULE, {new_client, self(), Socket}).

-spec subscribe([key()]) -> {ok, sub_id()} | {error, term()}.
subscribe(Keys) ->
    subscribe(Keys, vectorclock:new()).

-spec subscribe([key()], antidote:snapshot_time()) ->
          {ok, sub_id()} | {error, term()}.
subscribe(Keys, Snapshot) when is_list(Keys) ->
    gen_server:call(?MODULE, {subscribe, Keys, Snapshot}, infinity).

-spec unsubscribe(sub_id()) -> ok | {error, term()}.
unsubscribe(SubId) ->
    gen_server:cast(?MODULE, {unsubscribe, SubId}).

-spec notify_keys(keys_notif()) -> ok.
notify_keys(KeyNotification) ->
    gen_server:cast(?MODULE, KeyNotification).

-spec notify_pings(ping_notif()) -> ok.
notify_pings(PingNotification) ->
    gen_server:cast(?MODULE, PingNotification).


%%------------------------------------------------------------------------------

init(_) ->
    {ok, #state{ sn2subs = new_snapshot_queue()} }.

handle_call({subscribe, Keys, Snapshot} = Msg, {Pid, _}, State) ->
    ?LOG_DEBUG("subscribe request ~p from ~p~n", [Msg, Pid]),
    case is_active_client(Pid, State) of
        true ->
            {SubId, State1} = add_sub_int(Pid, Keys, Snapshot, State),
            {reply, {ok, SubId}, State1};
        false ->
            ?LOG_WARNING("calling subscribe without registration ~p~n", [Pid, State]),
            {reply, {error, unknown_client}, State}
    end;
handle_call({unsubscribe, SubId} = Msg, {Pid, _}, State) ->
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
handle_call(Msg, {Pid, _}, State) ->
    ?LOG_WARNING("unknown hand_call ~p from ~p~n", [Msg, Pid]),
    {reply, {error, not_implemented}, State}.

handle_cast({new_client, Pid, Socket}, State) ->
    ?LOG_DEBUG("new client ~p~n", [Pid]),
    State1 = add_client_int(Pid, Socket, State),
    {noreply, State1};

handle_cast(#keys_notif{keys = Keys, snapshot = Sn, partition = Partition} = E, State0) ->
    ?LOG_DEBUG("key notification ~p~n", [E]),
    {PSn, State1} = update_partial_snapshot(Sn, Partition, State0),

    SubID2Keys = get_subids_map(Sn, Keys, State0),
    maybe_keys_notifications(SubID2Keys, State0),

    SnOrdered1 = add_snapshot_to_queue(Sn, maps:keys(SubID2Keys), State1#state.sn2subs),
    {_, SnOrdered2} = maybe_snapshot_notifications(PSn, SnOrdered1),

    {noreply, State1#state{ sn2subs = SnOrdered2 }};
handle_cast(#ping_notif{snapshot = Sn, partition = Partition} = E, State0) ->
    ?LOG_DEBUG("ping notification ~p~n", [E]),
    {PSn, State1} = update_partial_snapshot(Sn, Partition, State0),

    {_, SnOrdered2} = maybe_snapshot_notifications(PSn, State1#state.sn2subs),
    {noreply, State1#state{ sn2subs = SnOrdered2 }};

handle_cast(Msg, State) ->
    ?LOG_WARNING("unknown hand_cast ~p~n", [Msg]),
    {noreply, State}.

handle_info({'DOWN', _MonRef, process, Pid, _}, State) ->
    State1 = cleanup_client_int(Pid, State),
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
    Clients1 = lists:keystore(Pid, 1, Clients, {Pid, {Socket, MonRef, []}}),
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
        {value, {Pid, {_Socket, MonRef, SubIds}}, Clients1} ->
            catch erlang:demonitor(MonRef),
            {Subs1, Keys2Subs1} =
                lists:foldl(
                  fun(SubId, {SubsA, Key2SubsA}) ->
                      {value, {_SubId, {Keys, _SN, Pid}}, SubsA1} =
                          lists:keytake(SubId, 1, SubsA),
                      Keys2SubsA1 = lists:foldl(
                                      fun(Key, Key2Subs) ->
                                              case Key2Subs of
                                                  M = #{ Key := [S] } when S =:= SubId ->
                                                      maps:remove(Key, M);
                                                  M = #{ Key := V } ->
                                                      maps:put(Key, lists:delete(SubId, V), M)
                                              end
                                      end, Key2SubsA, Keys),
                      {SubsA1, Keys2SubsA1}
                  end, {State#state.subs, State#state.keys2subs}, SubIds),
            State#state{clients = Clients1,
                        subs = Subs1,
                        keys2subs = Keys2Subs1
                       };
        false ->
            State
    end.

%% Invariant - this call expects client to be in the state
add_sub_int(Pid, Keys, Snapshot, State) ->
    SubId = make_ref(),

    _ClientInfo = {_Pid, {P, M, S}} = lists:keyfind(Pid, 1, State#state.clients),
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

-spec get_subids_map(sn(), [key()], state()) ->
          #{ sub_id() => [key()] }.
get_subids_map(Sn, Keys, #state{keys2subs = T, subs = Subs}) ->
    FilterSubs = maps:with(Keys, T),
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

-spec update_partial_snapshot(sn(), partition(), state()) ->
    { sn(), state() }.
update_partial_snapshot(Sn, Pt, #state{parts = Parts0} = State) ->
    Parts1 = Parts0#{ Pt => Sn },
    {merge(Parts0), State#state{parts = Parts1}}.

new_snapshot_queue() ->
    vector_orddict:new().

-spec add_snapshot_to_queue(sn(), [sub_id()], vector_orddict:vector_orddict()) ->
          vector_orddict:vector_orddict().
add_snapshot_to_queue(Sn, SubIds, SnOrdered0) ->
    SnOrderedList = vector_orddict:to_list(SnOrdered0),
    case lists:keyfind(Sn, 1, SnOrderedList) of
        false ->
            vector_orddict:insert(Sn, SubIds, SnOrdered0);
        {Sn, SL} ->
            SnOrderedList1 =
                lists:keyreplace(Sn, 1, SnOrderedList, {Sn, lists:usort(SL ++ SubIds)}),
            vector_orddict:from_list(SnOrderedList1)
    end.

maybe_keys_notifications(SubID2Keys, #state{} = State) ->
    ok.

-spec maybe_snapshot_notifications(sn(), vector_orddict:vector_orddict()) ->
          { [{sn(), [sub_id()]}],  vector_orddict:vector_orddict()}.
maybe_snapshot_notifications(PSn, SnOrdered0) ->
    ReverseOrder = lists:reverse( vector_orddict:to_list(SnOrdered0) ),
    {ToNotify, SnOrderRemain} =
        dropsplit(
          fun({Sn, SubIds}, Acc) ->
                  case vectorclock:ge(PSn, Sn) of
                      true ->
                          {true, [{Sn, SubIds} | Acc]};
                      false ->
                          false
                  end
          end, [], ReverseOrder),
    {lists:reverse(ToNotify), vector_orddict:from_list(
                                lists:reverse(SnOrderRemain))}.

dropsplit(Fun, InitAcc, [H | Rest] = L) ->
    case Fun(H, InitAcc) of
        {true, Acc} ->
            dropsplit(Fun, Acc, Rest);
        false ->
            {InitAcc, L}
    end;
dropsplit(_Fun, Init, []) ->
    {Init, []}.


send_snapshot_notification(SN, SubIds) ->
    ok.

%% FIXME: Probably all_defined should be called on proxies only
merge(VcMap) ->
    case all_defined(VcMap) of
        true ->
            vectorclock:min(maps:values(VcMap));
        false ->
            ?LOG_ERROR("vectorclock with missing entries: ~p", [VcMap]),
            erlang:error({malformed_vv, VcMap})
    end.

all_defined(VcMap) ->
    maps:fold(fun(_K, V, Acc) -> Acc andalso V =/= undefined end, true, VcMap).


-ifdef(EUNIT).

-define(k(I), erlang:list_to_binary(io_lib:format("key~p",[I]))).
-define(v(A, B, C), vectorclock:from_list([{dc1, A}, {dc2, B}, {dc3, C}])).

simple_new_client_test() ->
    erlang:process_flag(trap_exit, true),

    {ok, Pid} = vx_subs_server:start_link(),
    SubPid = spawn_link(
               fun() ->
                       ok = vx_subs_server:add_new_client(none),
                       {ok, SubId} = vx_subs_server:subscribe([ ?k(1), ?k(2), ?k(3) ]),
                       ok = vx_subs_server:unsubscribe(SubId),
                       exit(normal)
               end),
    receive
        {'EXIT', SubPid, normal} ->
            ok
    after 1000 ->
            erlang:error(timeout)
    end,
    ?assertEqual(ok, ok).

simple_snapshot_queue_test() ->
    Q0 = new_snapshot_queue(),
    Q1 = add_snapshot_to_queue(?v(1,1,1), [ref1], Q0),
    Q2 = add_snapshot_to_queue(?v(1,2,1), [ref2], Q1),
    Q3 = add_snapshot_to_queue(?v(1,3,1), [ref4, ref1], Q2),
    Q4 = add_snapshot_to_queue(?v(2,2,1), [ref2], Q3),
    Q5 = add_snapshot_to_queue(?v(2,1,1), [ref1], Q4),

    {Notify, Remaining} = maybe_snapshot_notifications(?v(2,1,1), Q5),
    ?assertEqual([
                  {?v(1,1,1), [ref1]},
                  {?v(2,1,1), [ref1]}
                 ], Notify
                ),
    ?assertEqual([
                  {?v(1,2,1), [ref2]},
                  {?v(2,2,1), [ref2]},
                  {?v(1,3,1), [ref4, ref1]}
                 ], lists:reverse(vector_orddict:to_list(Remaining))).

-endif.
