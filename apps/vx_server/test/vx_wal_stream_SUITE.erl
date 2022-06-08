-module(vx_wal_stream_SUITE).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("vx_client/include/vx_proto.hrl").

-define(APP, vx_server).
-define(comment(A), [{userdata,
                      [{description, A}]
                     }]).

all() ->
    [ %%single_node_test,
      {group, setup_node}
    ].

groups() ->
    [{setup_node, [],
      [ {setup_meck, [], [sanity_check,
                          single_txn,
                          single_txn_from_history,
                          single_txn_from_history2
                         ]
        },
        single_txn_via_client,
        multiple_txns_via_client,
        merged_values_within_txn
      ]}
    ].

init_per_suite(_, Config) ->
    Config.

end_per_suite(_, Config) ->
    Config.

init_per_group(setup_node, Config) ->
    vx_test_utils:init_testsuite(),
    {ok, _App} = vx_test_utils:init_clean_node(
                   node(), 0,
                   [{ riak_core, ring_creation_size, 1 },
                    { antidote, sync_log, true}
                   ]),
    Config;
init_per_group(setup_meck, _Config) ->
    meck:new(vx_wal_tcp_worker, [no_link, passthrough]),
    meck:expect(vx_wal_tcp_worker, send,
                fun(Port, TxId, TxOpsList) ->
                        Port ! {wal_msg, TxId, TxOpsList}, true
                end).

end_per_group(setup_meck, Config) ->
    meck:unload(vx_wal_tcp_worker),
    Config;
end_per_group(setup_node, Config) ->
    Config.

%%------------------------------------------------------------------------------

sanity_check() ->
    ?comment("Sanity check for replication").
sanity_check(_Config) ->
    {ok, Pid} = vx_wal_stream:start_link([]),
    {ok, _}   = vx_wal_stream:start_replication(Pid, self(), []),
    ok = vx_wal_stream:stop_replication(Pid),

    {ok, Pid2} = vx_wal_stream:start_link([]),
    {ok, _} = vx_wal_stream:start_replication(Pid2, self(), []),
    ok = vx_wal_stream:stop_replication(Pid2),
    ok.

single_txn() ->
    ?comment("Single transaction is replicated").

single_txn(_Config) ->
    {Key, Bucket, Type, Value} = {key_a, <<"my_bucket">>, antidote_crdt_counter_pn, 5},
    {ok, SN} = simple_update_value([{Key, Value}], Bucket),
    assert_receive(100),

    {ok, Pid} = vx_wal_stream:start_link([]),
    {ok, _}   = vx_wal_stream:start_replication(Pid, self(), []),

    [Msg] = assert_count(1, 1000),
    ct:log("message: ~p~n", [Msg]),

    K = key_format(Key, Bucket),
    ?assertMatch({wal_msg, _Txn, [{K, Type, Value}]}, Msg),

    ok = vx_wal_stream:stop_replication(Pid).

single_txn_from_history() ->
    ?comment("When database has single transaction we get it when starting replication").
single_txn_from_history(_Config) ->
    {Key, Bucket, Type, Value} = {key_a, <<"my_bucket">>, antidote_crdt_counter_pn, 5},

    {ok, Pid} = vx_wal_stream:start_link([]),
    {ok, _}   = vx_wal_stream:start_replication(Pid, self(), []),
    [Msg] = assert_count(1, 1000),
    ct:log("message: ~p~n", [Msg]),

    K = key_format(Key, Bucket),
    ?assertMatch({wal_msg, _Txn, [{K, Type, Value}]}, Msg),

    assert_receive(100),

    ok = vx_wal_stream:stop_replication(Pid).

single_txn_from_history2() ->
    ?comment("When database has single transaction we get it when starting replication").
single_txn_from_history2(_Config) ->
    {Key, Bucket, Type, Value} = {key_a, <<"my_bucket">>, antidote_crdt_counter_pn, 5},

    {ok, Pid} = vx_wal_stream:start_link([]),
    {ok, _}   = vx_wal_stream:start_replication(Pid, self(), []),
    [Msg] = assert_count(1, 1000),
    ct:log("~p message: ~p~n", [?LINE, Msg]),

    K = key_format(Key, Bucket),
    ?assertMatch({wal_msg, _Txn, [{K, Type, 5}]}, Msg),

    assert_receive(100),

    {ok, SN1} = simple_update_value([{Key, 6}], Bucket),
    [Msg1] = assert_count(1, 1000),
    ct:log("~p message: ~p~n", [?LINE, Msg1]),

    ?assertMatch({wal_msg, _Txn, [{K, Type, 11}]}, Msg1),

    assert_receive(100),
    ok = vx_wal_stream:stop_replication(Pid).

single_txn_via_client() ->
    ?comment("").
single_txn_via_client(Config) ->
    {Key, Bucket, Type, Value} = {key_d, <<"client_bucket">>, antidote_crdt_counter_pn, 5},

    {ok, SN1} = simple_update_value([{Key, 5}], <<"client_bucket">>),

    {ok, C} = vx_client:connect("127.0.0.1",
                                vx_test_utils:port(vx_server, pb_port, 0), []),
    ok = vx_client:start_replication(C, []),
    M = [_, _, Msg] = assert_count(3, 1000),
    ct:log("~p message: ~p~n", [?LINE, M]),

    K = key_format(Key, Bucket),
    ?assertMatch(#vx_client_msg{pid = C,
                                msg = #vx_wal_txn{ txid = _Txid,
                                                   ops = [{K, Type, 5}]
                                                 }}, Msg),
    ok = vx_client:stop_replication(C),

    {ok, _SN2} = simple_update_value([{Key, 6}], <<"client_bucket">>),
    assert_receive(100),
    ok.

multiple_txns_via_client() ->
    ?comment("").
multiple_txns_via_client(_Config) ->
    {Key, Bucket, Type, Value} = {key_d, <<"client_bucket">>, antidote_crdt_counter_pn, 11},

    {ok, C} = vx_client:connect("127.0.0.1",
                                vx_test_utils:port(vx_server, pb_port, 0), []),
    ok = vx_client:start_replication(C, []),

    M = [_, _, Msg1, Msg2] = assert_count(4, 1000),
    ct:log("~p messages: ~p~n~p~n", [?LINE, Msg1, Msg2]),

    K = key_format(Key, Bucket),
    ?assertMatch(#vx_client_msg{pid = C,
                                msg = #vx_wal_txn{ txid = _Txid,
                                                   ops = [{K, Type, 5}]
                                                 }}, Msg1),
    ?assertMatch(#vx_client_msg{pid = C,
                                msg = #vx_wal_txn{ txid = _Txid,
                                                   ops = [{K, Type, 11}]
                                                 }}, Msg2),

    {ok, SN1} = simple_update_value([{Key, 6}], <<"client_bucket">>),

    [Msg3] = assert_count(1, 1000),
    ?assertMatch(#vx_client_msg{pid = C,
                                msg = #vx_wal_txn{ txid = _Txid,
                                                   ops = [{K, Type, 17}]
                                                 }}, Msg3),

    ok = vx_client:stop_replication(C),
    ok.

merged_values_within_txn() ->
    ?comment("").
merged_values_within_txn(_Config) ->
    {Key, Bucket, Type, _Value} = {key_w, <<"u_bucket">>, antidote_crdt_counter_pn, 0},
    {ok, C} = vx_client:connect("127.0.0.1",
                                vx_test_utils:port(vx_server, pb_port, 0), []),
    ok = vx_client:start_replication(C, []),
    %% Skip first messages
    M = [_, _, _, _, _] = assert_count(5, 1000),

    {ok, _} = simple_update_value([{Key, 2}, {Key, 2}], Bucket),
    [Msg] = assert_count(1, 1000),
    ?assertMatch(#vx_client_msg{pid = C,
                                msg = #vx_wal_txn{ txid = _Txid,
                                                   ops = [{K, Type, 4}]
                                                 }}, Msg),
    ok = vx_client:stop_replication(C),
    ok.


%------------------------------------------------------------------------------

key_format(Key, Bucket) ->
    {erlang:atom_to_binary(Key, latin1), Bucket}.

simple_update_value(KVList, Bucket) ->
    {ok, Pid} = antidotec_pb_socket:start_link("127.0.0.1",
                                               vx_test_utils:port(antidote, pb_port, 0)),
    {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{static, false}]),

    UpdateOps =
        lists:map(
          fun({Key, Value}) ->
                  Identity = {erlang:atom_to_binary(Key, latin1),
                              antidote_crdt_counter_pn, Bucket},
                  Operation = antidotec_counter:increment(
                                Value, antidotec_counter:new()),
                  antidotec_counter:to_ops(Identity, Operation)
          end, KVList),

    antidotec_pb:update_objects(Pid, lists:flatten(UpdateOps), TxId),
    {ok, SnapshotTime}= antidotec_pb:commit_transaction(Pid, TxId),
    ct:log("committed txn ~p:~n ~p~n", [binary_to_term(SnapshotTime), UpdateOps]),
    {ok, binary_to_term(SnapshotTime)}.

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
