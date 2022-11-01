-module(vx_wal_stream_multidc_SUITE).

%% common_test callbacks
-export([
         init_per_suite/2,
         end_per_suite/2,
         init_per_group/2,
         end_per_group/2,
         all/0,
         groups/0
        ]).

-export([wal_replication_from_multiple_dc/1,
         interleavings_sanity_check/1,
         one_direction_sanity_check/1
        ]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("vx_client/include/vx_proto.hrl").

-define(vtest, vx_test_utils).

all() ->
    [
      {group, setup_cluster}
    ].

groups() ->
    [{setup_cluster, [],
      [
       wal_replication_from_multiple_dc,
       {setup_interleavings, [], [interleavings_sanity_check]},
       one_direction_sanity_check
      ]}
    ].

init_per_suite(_, Config) ->
    Config.

end_per_suite(_, Config) ->
    Config.

init_per_group(setup_cluster, Config) ->
    vx_test_utils:init_multi_dc(
     ?MODULE, Config,
     [ [ {dev11, []} ],
       [ {dev21, []} ]
     ],
     [{ riak_core, ring_creation_size, 1 },
      { antidote, sync_log, true}
     ]);

init_per_group(setup_interleavings, Config) ->
    Bucket = <<"mybucket">>,
    ct:comment("Setup interleaved transactions~n"),
    [Dev1, Dev2] = lists:flatten(?config(clusters, Config)),

    ?vtest:with_replication_con(Dev1,
      [{offset, eof}],
      fun(_) ->
              [T1, T2, T3, T4, T5] =
                  lists:map(
                    fun(Node) -> {ok, Apid} = ?vtest:an_connect(Node),
                                 {ok, TxId} = ?vtest:an_start_tx(Apid),
                                 {Apid, TxId}
                    end, [Dev1, Dev2, Dev1, Dev2, Dev1]),

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
              InterleaveTxs = [ {Msg#vx_wal_txn.txid,
                                 Msg#vx_wal_txn.wal_offset
                                } || Msg <- L0 ],
              [{interleave, InterleaveTxs} | Config
              ]
      end).

end_per_group(setup_interleavings, Config) ->
    Config;
end_per_group(setup_cluster, Config) ->
    Config.

wal_replication_from_multiple_dc(Config) ->
    ct:log("Local time: ~p~n", [os:timestamp()]),

    lists:foreach(
      fun(Node) ->
              V1 = rpc:call(Node, os, timestamp, []),
              ct:log("~p local time: ~p~n", [Node, V1])
      end, lists:flatten(proplists:get_value(nodes, Config))).

interleavings_sanity_check(Config) ->
    [Dev1, _Dev2] = lists:flatten(?config(clusters, Config)),
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
    ct:log("StartOffset: ~p~nInterleaveTxs: ~p~n", [WOffset, InterleaveTxs]),

    [_ | RestIncluded] = lists:dropwhile(fun({T, _}) -> T =/= TxId  end, InterleaveTxs),
    ct:log("Assume to receive: ~p~n", [RestIncluded]),

    ?vtest:with_replication_con(Dev1,
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

one_direction_sanity_check(Config) ->
    Bucket = <<"mybucket">>,
    [Dev1, Dev2] = lists:flatten(?config(clusters, Config)),

    ?vtest:with_replication_con(Dev1,
      [{offset, eof}],
      fun(_) ->
              {ok, APid} = ?vtest:an_connect(Dev2),
              {ok, TxId} = ?vtest:an_start_tx(APid),

              interleave_update_value(
                [ { {APid, TxId}, [{?FUNCTION_NAME, 88}] }],
                Bucket),

              L0 = ?vtest:assert_count(
                     1, 10000,
                     fun(#vx_client_msg{msg =
                                            #vx_wal_txn{ txid = _TxId0,
                                                         wal_offset = _WalOffset0,
                                                         ops = Ops
                                                       }}) ->
                             [ binary_to_atom(K, utf8) || {{K, _}, _, _, _} <- Ops ]
                     end),
              ?assertEqual(L0, [[?FUNCTION_NAME]])
      end).

%%------------------------------------------------------------------------------
%% helpfuns
%%------------------------------------------------------------------------------

-spec interleave_update_value([ {{pid(), term()}, [{atom(), term()}]} ],
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
                      {ok, _} = ?vtest:an_commit_tx(Pid, TxId),
                      ok = ?vtest:an_disconnect(Pid),
                      Remaining1;
                  true  ->
                      Remaining1
              end
      end, [ Handler || {Handler, _} <- KVList ], KVList).

