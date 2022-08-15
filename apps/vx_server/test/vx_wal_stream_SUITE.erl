-module(vx_wal_stream_SUITE).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("vx_client/include/vx_proto.hrl").

-define(vtest, vx_test_utils).
-define(APP, vx_server).

all() ->
    [ %%single_node_test,
      {group, setup_node}
    ].

groups() ->
    [{setup_node, [],
      [
       {setup_meck, [], [sanity_check,
                         single_txn,
                         single_txn_from_history,
                         single_txn_from_history2
                        ]
       },
       {client, [],
        [
         single_txn_via_client,
         multiple_txns_via_client,
         replication_from_position,
         replication_from_position_batch_api,
         replication_from_eof,
         {setup_interleavings, [],
          [interleavings_sanity_check || _ <- lists:seq(1, 5)]
         }
        ]
       }
      ]}
    ].

init_per_suite(_, Config) ->
    Config.

end_per_suite(_, Config) ->
    Config.

init_per_group(setup_node, Config) ->
    ?vtest:init_testsuite(),
    {ok, _App} = ?vtest:init_clean_node(
                   node(), 0,
                   [{ riak_core, ring_creation_size, 1 },
                    { antidote, sync_log, true}
                   ]),
    Config;
init_per_group(setup_meck, _Config) ->
    ct:comment("Group of tests that use mocked implementation"),
    meck:new(vx_wal_tcp_worker, [no_link, passthrough]),
    meck:expect(vx_wal_tcp_worker, send,
                fun(Port, _DcId, TxId, TxOffset, TxOpsList) ->
                        Port ! {wal_msg, TxId, TxOffset, TxOpsList}, true
                end),
     meck:expect(vx_wal_tcp_worker, send_start,
                fun(_, _, _) -> ok end);

init_per_group(client, Config) ->
    ct:comment("Group of tests that use vx_client"),
    Config;
init_per_group(setup_interleavings, Config) ->
    Bucket = <<"mybucket">>,
    ct:comment("Setup interleaved transactions~n"),

    ?vtest:with_replication_con(
      [{offset, eof}],
      fun(_) ->
              [T1, T2, T3, T4, T5] =
                  lists:map(
                    fun(_) -> {ok, Apid} = ?vtest:an_connect(),
                              {ok, TxId} = ?vtest:an_start_tx(Apid),
                              {Apid, TxId}
                    end, lists:seq(1, 5)),

              Interleavings =
                  [ {T1, [{key_1_a, 41}] },
                    {T2, [{key_2_a, 42}] },
                    {T3, [{key_3_a, 43}] },
                    {T5, [{key_x_a, 47}] },
                    {T2, [{key_4_a, 44}] },
                    {T1, [{key_5_a, 45}] },
                    {T4, [{key_6_a, 46}] }
                  ],

              interleave_update_value(Interleavings, Bucket),

              %% Skip first messages
              L0 = [_Msg1, _Msg2, _Msg3, _Msg4, _Msg5]
                  = ?vtest:assert_count_msg(5, 1000),
              L1 = lists:map(fun(Msg) ->
                                     [ binary_to_atom(K, utf8) ||
                                         {{K, _}, _, _, _} <- Msg#vx_wal_txn.ops
                                     ]
                             end, L0),
              ?assertEqual(L1, [ [key_3_a],
                                 [key_x_a],
                                 [key_4_a, key_2_a],
                                 [key_1_a, key_5_a],
                                 [key_6_a]
                               ]),
              InterleaveTxs = [ {Msg#vx_wal_txn.txid, Msg#vx_wal_txn.wal_offset} || Msg <- L0 ],
              [{interleave, InterleaveTxs} | Config]
      end).

end_per_group(setup_interleavings, Config) ->
    Config;
end_per_group(client, Config) ->
    Config;
end_per_group(setup_meck, Config) ->
    meck:unload(vx_wal_tcp_worker),
    Config;
end_per_group(setup_node, Config) ->
    Config.

%%------------------------------------------------------------------------------

sanity_check(_Config) ->
    ct:comment("Sanity check"),
    {ok, Pid} = vx_wal_stream:start_link([]),
    {ok, _}   = vx_wal_stream:start_replication(Pid, make_ref(), self(), []),
    ok = vx_wal_stream:stop_replication(Pid),

    {ok, Pid2} = vx_wal_stream:start_link([]),
    {ok, _} = vx_wal_stream:start_replication(Pid2, make_ref(), self(), []),
    ok = vx_wal_stream:stop_replication(Pid2),
    ok.

single_txn(_Config) ->
    ct:comment("Single transaction test without offset"),
    {Key, Bucket, Type, Value} = {key_a, <<"my_bucket">>, antidote_crdt_counter_pn, 5},
    {ok, _SN} = simple_update_value([{Key, Value}], Bucket),
    ?vtest:assert_receive(100),

    {ok, Pid} = vx_wal_stream:start_link([]),
    {ok, _}   = vx_wal_stream:start_replication(Pid, make_ref(), self(), []),

    [Msg] = ?vtest:assert_count(1, 1000),
    ct:log("message: ~p~n", [Msg]),

    K = key_format(Key, Bucket),
    Ops = [5],
    ?assertMatch({wal_msg, _Txn, _Offset, [{K, Type, Value, Ops}]}, Msg),

    ok = vx_wal_stream:stop_replication(Pid).

single_txn_from_history(_Config) ->
    ct:comment("Single transaction test without offset"),
    {Key, Bucket, Type, Value} = {key_a, <<"my_bucket">>, antidote_crdt_counter_pn, 5},

    {ok, Pid} = vx_wal_stream:start_link([]),
    {ok, _}   = vx_wal_stream:start_replication(Pid, make_ref(), self(), []),
    [Msg] = ?vtest:assert_count(1, 1000),
    ct:log("message: ~p~n", [Msg]),

    K = key_format(Key, Bucket),
    Ops = [5],
    ?assertMatch({wal_msg, _Txn, _Offset, [{K, Type, Value, Ops}]}, Msg),

    ?vtest:assert_receive(100),

    ok = vx_wal_stream:stop_replication(Pid).

single_txn_from_history2(_Config) ->
    ct:comment("Single transaction test without offset, followed by new transaction"),
    {Key, Bucket, Type, _Value} = {key_a, <<"my_bucket">>, antidote_crdt_counter_pn, 5},

    {ok, Pid} = vx_wal_stream:start_link([]),
    {ok, _}   = vx_wal_stream:start_replication(Pid, make_ref(), self(), []),
    [Msg] = ?vtest:assert_count(1, 1000),
    ct:log("~p message: ~p~n", [?LINE, Msg]),

    K = key_format(Key, Bucket),
    ?assertMatch({wal_msg, _Txn, _Offset, [{K, Type, 5, [5]}]}, Msg),

    ?vtest:assert_receive(100),

    {ok, _SN1} = simple_update_value([{Key, 6}], Bucket),
    [Msg1] = ?vtest:assert_count(1, 1000),
    ct:log("~p message: ~p~n", [?LINE, Msg1]),

    Ops2 = [6],
    ?assertMatch({wal_msg, _Txn, _Offset, [{K, Type, 11, Ops2}]}, Msg1),

    ?vtest:assert_receive(100),
    ok = vx_wal_stream:stop_replication(Pid).

single_txn_via_client(_Config) ->
    ct:comment("Single transaction tests without offset"
               "followed by new trans after replication stopped"),
    {Key, Bucket, Type, _Value} = {key_d, <<"client_bucket">>, antidote_crdt_counter_pn, 5},

    {ok, _SN1} = simple_update_value([{Key, 5}], <<"client_bucket">>),

    {ok, C} = ?vtest:vx_connect(),
    ok = vx_client:start_replication(C, []),
    M = [_, _, Msg] = ?vtest:assert_count(3, 1000),
    ct:log("~p message: ~p~n", [?LINE, M]),

    K = key_format(Key, Bucket),
    ?assertMatch(#vx_client_msg{pid = C,
                                msg = #vx_wal_txn{ txid = _Txid,
                                                   ops = [{K, Type, 5, [5]}]
                                                 }}, Msg),
    ok = vx_client:stop_replication(C),

    {ok, _SN2} = simple_update_value([{Key, 6}], <<"client_bucket">>),
    ?vtest:assert_receive(100),
    ok.

multiple_txns_via_client(_Config) ->
    ct:comment("Multiple transactions test without offset"),
    {Key, Bucket, Type, _Value} = {key_d, <<"client_bucket">>, antidote_crdt_counter_pn, 11},

    {ok, C} = ?vtest:vx_connect(),
    ok = vx_client:start_replication(C, []),

    [_, _, Msg1, Msg2] = ?vtest:assert_count_msg(4, 1000),
    ct:log("~p messages: ~p~n~p~n", [?LINE, Msg1, Msg2]),

    K = key_format(Key, Bucket),
    ?assertMatch(#vx_wal_txn{ txid = _Txid,
                              ops = [{K, Type, 5, [5]}]
                            }, Msg1),
    ?assertMatch(#vx_wal_txn{ txid = _Txid,
                              ops = [{K, Type, 11, [6]}]
                            }, Msg2),

    {ok, _SN1} = simple_update_value([{Key, 6}], <<"client_bucket">>),

    [Msg3] = ?vtest:assert_count_msg(1, 1000),
    ?assertMatch(#vx_wal_txn{ txid = _Txid,
                              ops = [{K, Type, 17, [6]}]
                            }, Msg3),

    ok = vx_client:stop_replication(C),
    ok.

replication_from_position(_Config) ->
    ct:comment("Test replication from the middle of the transaction list"),
    {Key, Bucket, Type, _Value} = {key_w, <<"u_bucket">>, antidote_crdt_counter_pn, 0},
    K = key_format(Key, Bucket),

    [Msg4, Msg5, Msg6] =
        ?vtest:with_replication_con(
          [{offset, 0}],
          fun(C) ->
                  %% Skip first messages
                  [_, _, _, Msg4, Msg5] = ?vtest:assert_count_msg(C, 5, 1000),

                  {ok, _} = simple_update_value([{Key, 2}, {Key, 2}], Bucket),
                  [Msg6] = ?vtest:assert_count_msg(1, 1000),
                  ?assertMatch(#vx_wal_txn{ txid = _Txid,
                                            ops = [{K, Type, 4, [2, 2]}]
                                          }, Msg6),
                  [Msg4, Msg5, Msg6]
          end),

    WalOffset = Msg4#vx_wal_txn.wal_offset,
    ct:log("Expect to receive some messages, start with offset ~p~n", [WalOffset]),

    ?vtest:with_replication_con(
      [{offset, WalOffset}],
      fun(C1) ->
              [Msg41, Msg51, Msg61] = ?vtest:assert_count_msg(C1, 3, 1000),
              ?assertEqual([Msg4, Msg5, Msg6], [Msg41, Msg51, Msg61])
      end).

replication_from_position_batch_api(_Config) ->
    ct:comment("Test replication from the middle of the transaction list"),
    {Key, Bucket, Type, _Value} = {key_w, <<"u_bucket">>, antidote_crdt_counter_pn, 0},
    K = key_format(Key, Bucket),

    ?vtest:with_replication_con(
       [{offset, 0}, {sync, 3}],
       fun(C) ->
                  %% Skip first messages
               [_, _, _] = ?vtest:assert_count_msg(C, 3, 1000),
               ok = vx_client:get_next_stream_bulk(C, 3),
               [Msg4, Msg5, Msg6] = ?vtest:assert_count_msg(3, 1000),

               ?assertMatch(#vx_wal_txn{ txid = _Txid,
                                         ops = [{K, Type, 4, [2, 2]}]
                                       }, Msg6),
               [Msg4, Msg5, Msg6]
          end).

replication_from_eof(_Config) ->
    ct:comment("Test replication from eof of file"),
    {Key, Bucket, _Type, _Value} = {?FUNCTION_NAME, <<"u_bucket">>,
                                   antidote_crdt_counter_pn, 0},
    ?vtest:with_replication_con(
      [{offset, eof}],
      fun(_) ->
              ?vtest:assert_receive(1000),

              lists:foreach(
                fun(_) ->
                        {ok, _} = simple_update_value([{Key, 2}, {Key, 2}], Bucket)
                end, lists:seq(1, 2)),

              %% Skip first messages
              [_Msg1, _Msg2] = ?vtest:assert_count_msg(2, 1000)
      end),
    ok.

interleavings_sanity_check(Config) ->
    NextPos =
        try
            {?FUNCTION_NAME, OldConfig} = ?config(saved_config, Config),
            ?config(next_pos, OldConfig)
        catch _:_ ->
                1
        end,
    InterleaveTxs = ?config(interleave, Config),

    ct:comment("Start replication from tx at position ~p out of ~p",
               [NextPos, length(InterleaveTxs)]),

    {TxId, WOffset} = lists:nth(NextPos, InterleaveTxs),
    RestIncluded = lists:dropwhile(fun({T, _}) -> T =/= TxId  end, InterleaveTxs),

    ?vtest:with_replication_con(
      [{offset, WOffset}],
      fun(_) ->
              L0 = ?vtest:assert_count(
                     length(RestIncluded), 1000,
                     fun(#vx_client_msg{msg =
                                            #vx_wal_txn{ txid = TxId0,
                                                         wal_offset = WalOffset0
                                                       }}) -> {TxId0, WalOffset0}
                                end),
              ?assertEqual(L0, RestIncluded)
      end),
    {save_config, [{next_pos, NextPos + 1} | proplists:delete(next_pos, Config)]}.

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
    antidotec_pb_socket:stop(Pid),
    {ok, binary_to_term(SnapshotTime)}.

-spec interleave_update_value([ {{pid(), antidote:txid()}, [{term(), term()}]} ],
                              antidote:bucket()) ->
          ok.
interleave_update_value(KVList, Bucket) ->
    lists:foldl(
      fun({{Pid, TxId}, KVList0}, Remaining) ->
              UpdateOps =
                  lists:map(
                    fun({Key, Value}) ->
                            Identity = {erlang:atom_to_binary(Key, latin1),
                                        antidote_crdt_counter_pn, Bucket},
                            Operation = antidotec_counter:increment(
                                          Value, antidotec_counter:new()),
                            antidotec_counter:to_ops(Identity, Operation)
                    end, KVList0),
              antidotec_pb:update_objects(Pid, lists:flatten(UpdateOps), TxId),

              Remaining1 = lists:delete({Pid, TxId}, Remaining),
              case lists:member({Pid, TxId}, Remaining1) of
                  false ->
                      {ok, _}= antidotec_pb:commit_transaction(Pid, TxId),
                      ok = antidotec_pb_socket:stop(Pid),
                      Remaining1;
                  true  ->
                      Remaining1
              end
      end, [ Handler || {Handler, _} <- KVList ], KVList).
