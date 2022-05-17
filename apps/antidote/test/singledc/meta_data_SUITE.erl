-module(meta_data_SUITE).
% common_test callbacks
-export([
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0
        ]).

-export([
         meta_data_notif_test1/1
        ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("meta_data_notif_server.hrl").

init_per_suite(Config) ->
    test_utils:at_init_testsuite(),
    meck:new(meta_data_notif_server, [no_link, passthrough]),
    meck:expect(meta_data_notif_server, notify,
                fun(A, B) -> meck:passthrough([A, B]) end
               ),

    {ok, Apps} = test_utils:init_node(dev3, node()),
    [{apps, Apps} | Config].

end_per_suite(_Config) ->
    [application:stop(App)
     || App <- [ranch, poolboy, riak_core, erlzmq, prometheus,
                elli, elli_prometheus, prometheus_process_collector,
                antidote_stats,
                antidote]
    ],
    meck:unload(meta_data_notif_server).


init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(Name, _) ->
    ct:print("[ OK ] ~p", [Name]),
    ok.

all() ->
    [
     meta_data_notif_test1
    ].

meta_data_notif_test1(_Config) ->
    meck:wait(1, meta_data_notif_server, notify, '_', 2000),
    LocalDc = dc_utilities:get_my_dc_id(),
    {ok, #{LocalDc := Value} = SN1} =
        dc_utilities:get_stable_snapshot([include_local_dc]),
    ?assertEqual(true, Value =/= undefined),

    ok = meta_data_notif_server:subscribe_to_stable(SN1),
    receive
        #md_stable_snapshot{sn = SN1} ->
            ok
    after 2000 ->
            erlang:error({timeout_stable, SN1, dc_utilities:get_stable_snapshot() })
    end.
