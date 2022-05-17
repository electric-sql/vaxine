%% @doc Meta data notification server. Allows client code to subscribe to the
%% event when the provided snapshot becomes stable.

-module(meta_data_notif_server).

-include("antidote.hrl").
-include("meta_data_notif_server.hrl").

-ifdef(EUNIT).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([start_link/0, start/0,
         init/0,
         loop/1,
         subscribe_to_stable/1,
         notify/2
        ]).

-record(state, { mons = [] :: [{pid(), reference()}],
                 %% NOTE: vector_orddict stores Big -> Small
                 snaps = vector_orddict:new() ::
                           vector_orddict:vector_orddict([pid()]),
                 min_snapshot :: undefined | antidote:snapshot_time()
               }).
-type state() :: #state{}.

start_link() ->
    proc_lib:start_link(?MODULE, init, []).

start() ->
    proc_lib:start(?MODULE, init, []).

%% @doc API to subscribe to stable snapshot notifications. Notification server
%% will send notification of the form `#md_stable_snapshot{}` to the calling process.
%% Same client may subscribe to multiple snapshots at the same time.
-spec subscribe_to_stable(antidote:snapshot_time()) -> ok.
subscribe_to_stable(Snapshot) ->
    whereis(?MODULE) ! {add_snapshot, Snapshot, self()},
    ok.

%% @doc Meta data server notifies notif server about new stable snapshot.
-spec notify(stable_time_functions, antidote:snapshot_time()) -> ok.
notify(stable_time_functions, Snapshot) ->
    true = maybe_notify_about_snapshot(Snapshot),
    whereis(?MODULE) ! {stable_snapshot, Snapshot},
    ok.

%%------------------------------------------------------------------------------

init() ->
    try erlang:register(?MODULE, self()) of
        true ->
            proc_lib:init_ack({ok, self()}),
            loop(#state{})
    catch _:_ ->
            proc_lib:init_ack({error, already_registered})
    end.

loop(State) ->
    receive
        {add_snapshot, Snapshot, Pid} ->
            ?MODULE:loop(add_snapshot(Snapshot, Pid, State));

        {stable_snapshot, SN}
          when (State#state.min_snapshot =/= undefined) ->
            SN1 = wait_for_latest(SN),
            ?MODULE:loop(notify_subscribers(SN1, State));

        {stable_snapshot, _} ->
            ?MODULE:loop(State);

        {'DOWN', _MonRef, process, Pid, _} ->
            ?MODULE:loop(remove_subscriber(Pid, State#state.mons));

        stop ->
            unregister(?MODULE),
            exit(normal)
    end.

%%------------------------------------------------------------------------------

wait_for_latest(SN0) ->
    receive
        {stable_snapshot, SN1} ->
            wait_for_latest(SN1)
    after 0 ->
            SN0
    end.

-spec notify_subscribers(antidote:snapshot_time(), state()) -> state().
notify_subscribers(Snapshot, State) ->
    Snaps0 = vector_orddict:to_list(State#state.snaps),
    SnapsList0 = lists:reverse(Snaps0), %% Small -> Large
    case notify_subscribers(Snapshot, _Left = [], SnapsList0, []) of
        [] ->
            State#state{snaps = vector_orddict:new(), min_snapshot = undefined};
        SnapsList1 ->
            MinSnaps = calculate_min(SnapsList1),
            Snaps1 = vector_orddict:from_list( lists:reverse(SnapsList1) ),
            State#state{snaps = Snaps1, min_snapshot = MinSnaps}
    end.

%% List is expected to be sorted in ascending order
-spec calculate_min([{vectorclock:vectorclock(), term()}]) -> vectorclock:vectorclock().
calculate_min([{ Min, _} | Rest]) ->
    calculate_min(Min, Rest).

calculate_min(Min, []) ->
    Min;
calculate_min(Min, [{H, _} | Rest]) ->
    case vectorclock:ge(H, Min) of
        true ->
            Min;
        false ->
            calculate_min(vectorclock:min2(Min, H), Rest)
    end.

notify_subscribers(_Snapshot, Concurrent, [], Notifications) ->
    send_snapshot_notifications(lists:reverse(Notifications)),
    lists:reverse(Concurrent);
notify_subscribers(Snapshot, Concurrent, [{SN, Clients} = R | MaybeGreater] = Snaps,
                  Notifications) ->
    case vectorclock:ge(Snapshot, SN) of
        true ->
            notify_subscribers(Snapshot, Concurrent, MaybeGreater,
                               [{SN, Clients} | Notifications]);
        false ->
            case vectorclock:ge(SN, Snapshot) of
                true ->
                    send_snapshot_notifications(lists:reverse(Notifications)),
                    lists:reverse(Concurrent) ++ Snaps;
                false ->
                    notify_subscribers(Snapshot, [R | Concurrent], MaybeGreater,
                                       Notifications)
            end
    end.

send_snapshot_notifications([{SN, Clients} | Rest]) ->
    _ = [Client ! #md_stable_snapshot{sn = SN} || Client <- Clients],
    send_snapshot_notifications(Rest);
send_snapshot_notifications([]) ->
    ok.

add_snapshot(Snapshot, Pid, State) ->
    #state{mons = Mons0, snaps = Snaps0, min_snapshot = MinSnap0} = State,
    Mons1 = add_subscriber(Pid, Mons0),

    {MinSnap1, Snaps1} = add_snapshot(Pid, Snapshot, MinSnap0, Snaps0),

    State#state{mons = Mons1, snaps = Snaps1, min_snapshot = MinSnap1}.

add_subscriber(Pid, Mons0) ->
    case lists:keyfind(Pid, 1, Mons0) of
        {Pid, _} -> Mons0;
        false ->
            MonRef = erlang:monitor(process, Pid),
            [{Pid, MonRef} | Mons0]
    end.

-spec add_snapshot(pid(), antidote:snapshot_time(), antidote:snapshot_time(),
                   vector_orddict:vector_orddict([pid()])) ->
          { antidote:snapshot_time(), vector_orddict:vector_orddict([pid()]) }.
add_snapshot(Pid, Snapshot, MinSnap, Snaps0) ->
    Snaps1 = vector_orddict:append(Snapshot, Pid, Snaps0),
    case MinSnap of
        undefined ->
            Min = calculate_min(
                    lists:reverse(
                      vector_orddict:to_list(Snaps1))),
            { Min, Snaps1 };
        _ ->
            { vectorclock:min2(MinSnap, Snapshot), Snaps1 }
    end.

remove_subscriber(Pid, Mons0) ->
    case lists:keytake(Pid, 1, Mons0) of
        {value, {_Pid, MonRef}, Mons1} ->
            erlang:demonitor(MonRef, [flush]),
            Mons1;
        false ->
            Mons0
    end.

-spec maybe_notify_about_snapshot(antidote:snapshot_time()) -> boolean().
maybe_notify_about_snapshot(_Snapsthot) ->
    %% NOTE: we may have here a simple check whether or not we need to
    %% notify meta_data_notif_server, but for now it seems to be an overkill
    %% so just notify every time
    true.

-ifdef(EUNIT).

-define(VC(A, B, C), vectorclock:from_list([{dc1, A}, {dc2, B}, {dc3, C}])).
-define(V_NUM, 2).

sanity_check_test() ->
    {ok, Pid} = ?MODULE:start(),
    MonRef = monitor(process, Pid),

    ok = ?MODULE:subscribe_to_stable(?VC(1, 1, 2)),
    ok = ?MODULE:subscribe_to_stable(?VC(2, 1, 1)),

    ok = ?MODULE:notify(stable_time_functions, ?VC(1, 1, 1)),
    assert_receive(100),

    ok = ?MODULE:subscribe_to_stable(?VC(2, 3, 2)),
    ok = ?MODULE:subscribe_to_stable(?VC(4, 2, 2)),

    ?MODULE:notify(stable_time_functions, ?VC(2, 2, 1)),

    V1 = ?VC(2, 1, 1),
    [#md_stable_snapshot{sn = V1}] = assert_count(1, 1000),

    ?MODULE:notify(stable_time_functions, ?VC(2, 2, 2)),

    V2 = ?VC(1, 1, 2),
    [#md_stable_snapshot{sn = V2}] = assert_count(1, 1000),
    assert_receive(100),

    Pid ! stop,
    receive {'DOWN', MonRef, process, Pid, _} -> ok end,
    ok.


vv_gen() ->
    vector(?V_NUM, range(1, 10)).

prop_notif_ordering() ->
    ?FORALL({NotifyVV0, VVList0},
            {vv_gen(), non_empty(list(vv_gen()))},
            begin
                {Pid, MonRef} =
                    spawn_monitor(
                      fun() ->
                              try prop_notif_ordering_worker(NotifyVV0, VVList0) of
                                  Bool -> exit(Bool)
                              catch _:_ ->
                                      exit(false)
                              end
                      end),
                receive
                    {'DOWN', MonRef, process, Pid, true} ->
                        true;
                    {'DOWN', MonRef, process, Pid, _}  ->
                        false
                end
            end).

prop_notif_ordering_worker(NotifyVV0, VVList0) ->
    NotifyVV1 = lists:zip(lists:seq(1, 2), NotifyVV0),
    NotifyVV2 = vectorclock:from_list(NotifyVV1),

    {ok, Pid} = ?MODULE:start(),
    MonRef = monitor(process, Pid),
    VVList1 = lists:map(
                fun(V) ->
                        VV0 = lists:zip(lists:seq(1, ?V_NUM), V),
                        VV1 = vectorclock:from_list(VV0),
                        ok = ?MODULE:subscribe_to_stable(VV1),
                        VV1
                end,
                lists:usort(VVList0)),
    ok = ?MODULE:notify(stable_time_functions, NotifyVV2),

    ExpectedNotif = lists:filter(
                      fun(A) ->
                              vectorclock:le(A, NotifyVV2)
                      end, VVList1),

    try
        ExpectedNotifLen = length(ExpectedNotif),
        Messages = assert_count(ExpectedNotifLen, 500),

        case ExpectedNotifLen of
            0 ->
                ok;
            _ ->
                %% verify order
                lists:foldl(
                  fun(#md_stable_snapshot{sn = Elem} = E, #md_stable_snapshot{sn = Prev}) ->
                          ?assertEqual(false, vectorclock:lt(Elem, Prev)),
                          E
                  end, hd(Messages), tl(Messages))
        end,

        assert_receive(10)
    of
        ok ->
            Pid ! stop,
            receive {'DOWN', MonRef, process, Pid, _} -> ok end,
            true
    catch _:_ ->
            Pid ! stop,
            receive {'DOWN', MonRef, process, Pid, Reason} -> ok end,
            io:format("state: ~p~n", [Reason]),
            false
    end.

meta_data_notif_manual_test() ->
    ?assertEqual(true, prop_notif_ordering_worker([7, 10], [[5, 4], [6, 10]])).

meta_data_notif_manual2_test() ->
    ?assertEqual(true, prop_notif_ordering_worker([2, 2], [[1, 2], [2, 1], [1, 1]])).

meta_data_notif_manual3_test() ->
    ?assertEqual(true, prop_notif_ordering_worker([10, 4], [[8, 1], [6, 3]])).

meta_data_notif_client_test_() ->
     {timeout, 600,
      ?_assertEqual(true,
                   proper:quickcheck(
                     prop_notif_ordering(), [1000, {to_file, user}])
                  )
     }.

assert_receive(Timeout) ->
    receive
        M ->
            erlang:error({unexpected_msg, M})
    after Timeout ->
            ok
    end.

assert_count(0, _Timeout) ->
    [];
assert_count(N, Timeout) ->
    receive
        M ->
            [M | assert_count(N-1, Timeout)]
    after Timeout ->
              erlang:error({not_sufficient_msg_count, N})
    end.

-endif.
