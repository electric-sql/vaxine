-module(vx_test_utils).

-export([init_multi_dc/4,
         start_node/4,
         init_clean_node/2,
         init_clean_node/3,
         init_testsuite/0,
         port/3,
         node_modifier/1
        ]).

-export([ an_connect/0, an_connect/1,
          an_start_tx/1, an_commit_tx/2,
          an_disconnect/1,
          vx_connect/0, vx_connect/1,
          vx_disconnect/1,
          with_connection/1, with_connection/2,
          with_replication/3,
          with_replication_con/2, with_replication_con/3
        ]).

-export([assert_count/2, assert_count/3,
         assert_count_msg/2, assert_count_msg/3,
         assert_receive/1
        ]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("vx_client/include/vx_proto.hrl").

-define(APP, vx_server).

init_multi_dc(Suite, Config, ClusterDcSetup, ErlangEnv) ->
    ct:pal("[~p]", [Suite]),

    init_testsuite(),
    Clusters = set_up_clusters_common([{suite_name, ?MODULE} | Config],
                                      ClusterDcSetup, ErlangEnv),
    Nodes = hd(Clusters),
    [{clusters, Clusters} | [{nodes, Nodes} | Config]].

start_node({Name, OsEnv}, PortModifier, Config, ErlangEnv) ->
    NodeConfig = [{monitor_master, true},
                  {env, OsEnv}
                 ],
    case ct_slave:start(Name, NodeConfig) of
        {ok, Node} ->
            %% code path for compiled dependencies
            CodePath = lists:filter(fun filelib:is_dir/1, code:get_path()) ,
            lists:foreach(fun(P) -> rpc:call(Node, code, add_patha, [P]) end, CodePath),

            init_clean_node(Node, PortModifier, ErlangEnv),
            ct:pal("Node ~p started with ports ~p-~p and config ~p~n",
                   [Name, PortModifier, PortModifier + 5, Config]),
            {connect, Node};
        {error, already_started, Node} ->
            ct:log("Node ~p already started, reusing node", [Node]),
            {ready, Node};
        {error, Reason, Node} ->
            ct:pal("Error starting node ~w, reason ~w, will retry", [Node, Reason]),
            ct_slave:stop(Name),
            time_utils:wait_until_offline(Node),
            start_node({Name, OsEnv}, PortModifier, Config, ErlangEnv)
    end.

init_clean_node(Node, PortModifier) ->
    ok = init_clean_load_applications(Node),
    init_clean_node_without_loading(Node, PortModifier),
    ensure_started(Node).

init_clean_node(Node, PortModifier, Environment) ->
    ok = init_clean_load_applications(Node),
    init_clean_node_without_loading(Node, PortModifier),
    [ rpc:call(Node, application, set_env, [ App, Key, Value ])
      || {App, Key, Value} <- Environment ],
    ensure_started(Node).

init_clean_load_applications(Node) ->
     % load application to allow for configuring the environment before starting
    ok = rpc:call(Node, application, load, [riak_core]),
    ok = rpc:call(Node, application, load, [antidote_stats]),
    ok = rpc:call(Node, application, load, [ranch]),
    ok = rpc:call(Node, application, load, [antidote]),
    ok = rpc:call(Node, application, load, [vx_server]),
    ok.

init_clean_node_without_loading(Node, PortModifier) ->
    %% get remote working dir of node
    {ok, NodeWorkingDir} = rpc:call(Node, file, get_cwd, []),

    %% DATA DIRS
    ok = rpc:call(Node, application, set_env,
                  [antidote, data_dir,
                   filename:join([NodeWorkingDir, Node, "antidote-data"])]),
    ok = rpc:call(Node, application, set_env,
                  [riak_core, ring_state_dir,
                   filename:join([NodeWorkingDir, Node, "data"])]),
    ok = rpc:call(Node, application, set_env,
                  [riak_core, platform_data_dir,
                   filename:join([NodeWorkingDir, Node, "data"])]),

    %% PORTS
    Port = 10000 + PortModifier * 10,
    ok = rpc:call(Node, application, set_env, [antidote, logreader_port, Port]),
    ok = rpc:call(Node, application, set_env, [antidote, pubsub_port, Port + 1]),
    ok = rpc:call(Node, application, set_env, [ranch, pb_port, Port + 2]),
    ok = rpc:call(Node, application, set_env, [riak_core, handoff_port, Port + 3]),
    ok = rpc:call(Node, application, set_env, [antidote_stats, metrics_port, Port + 4]),
    ok = rpc:call(Node, application, set_env, [?APP, pb_port, Port + 5]),

    %% LOGGING Configuration
    %% add additional logging handlers to ensure easy access to remote node logs
    %% for each logging level
    LogRoot = filename:join([NodeWorkingDir, Node, "logs"]),
    %% set the logger configuration
    ok = rpc:call(Node, application, set_env, [antidote, logger, log_config(LogRoot)]),
    %% set primary output level, no filter
    rpc:call(Node, logger, set_primary_config, [level, all]),
    %% load additional logger handlers at remote node
    rpc:call(Node, logger, add_handlers, [antidote]),

    %% redirect slave logs to ct_master logs
    ok = rpc:call(Node, application, set_env, [antidote, ct_master, node()]),
    ConfLog = #{level => debug, formatter => {logger_formatter, #{single_line => true, max_size => 2048}}, config => #{type => standard_io}},
    _ = rpc:call(Node, logger, add_handler, [antidote_redirect_ct, ct_redirect_handler, ConfLog]),

    %% ANTIDOTE Configuration
    %% reduce number of actual log files created to 4, reduces start-up time of node
    ok = rpc:call(Node, application, set_env, [riak_core, ring_creation_size, 4]),
    ok = rpc:call(Node, application, set_env, [antidote, sync_log, true]),
    ok.

ensure_started(Node) ->
    %% VX_SERVER Configuration
    rpc:call(Node, application, ensure_all_started, [?APP]).


log_config(LogDir) ->
    DebugConfig = #{level => debug,
        formatter => {logger_formatter, #{single_line => true, max_size => 2048}},
        config => #{type => {file, filename:join(LogDir, "debug.log")}}},

    InfoConfig = #{level => info,
        formatter => {logger_formatter, #{single_line => true, max_size => 2048}},
        config => #{type => {file, filename:join(LogDir, "info.log")}}},

    NoticeConfig = #{level => notice,
        formatter => {logger_formatter, #{single_line => true, max_size => 2048}},
        config => #{type => {file, filename:join(LogDir, "notice.log")}}},

    WarningConfig = #{level => warning,
        formatter => {logger_formatter, #{single_line => true, max_size => 2048}},
        config => #{type => {file, filename:join(LogDir, "warning.log")}}},

    ErrorConfig = #{level => error,
        formatter => {logger_formatter, #{single_line => true, max_size => 2048}},
        config => #{type => {file, filename:join(LogDir, "error.log")}}},

    [
        {handler, debug_antidote, logger_std_h, DebugConfig},
        {handler, info_antidote, logger_std_h, InfoConfig},
        {handler, notice_antidote, logger_std_h, NoticeConfig},
        {handler, warning_antidote, logger_std_h, WarningConfig},
        {handler, error_antidote, logger_std_h, ErrorConfig}
    ].

init_testsuite() ->
    {ok, Hostname} = inet:gethostname(),
    case net_kernel:start([list_to_atom("runner@" ++ Hostname), shortnames]) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok;
        {error, {{already_started, _}, _}} -> ok
    end.

port(antidote, logreader_port, N) -> port(N, 0);
port(antidote, pubsub_port, N)    -> port(N, 1);
port(antidote, pb_port, N)        -> port(N, 2);
port(antidote, handoff_port, N)   -> port(N, 3);
port(antidote, metric_port, N)    -> port(N, 4);
port(?APP,     pb_port, N)        -> port(N, 5).

port(PortModifier, SeqNum) ->
    10000 + PortModifier * 10 + SeqNum.

node_modifier(dev11) -> 1;
node_modifier(dev12) -> 2;
node_modifier(dev21) -> 3.

set_up_clusters_common(Config, ClusterDcSetup, ErlangEnv) ->
    StartDCs =
        fun(Nodes) ->
                %% start each node
                Cl = pmap(
                       fun({N, Env}) ->
                               PortModified = node_modifier(N),
                               start_node({N, Env}, PortModified, Config, ErlangEnv)
                       end, Nodes),
                [{Status, Claimant} | OtherNodes] = Cl,

                %% check if node was reused or not
                case Status of
                    ready -> ok;
                    connect ->
                        ct:pal("Creating a ring for claimant ~p and other nodes ~p", [Claimant, unpack(OtherNodes)]),
                        ok = rpc:call(Claimant, antidote_dc_manager, add_nodes_to_dc, [unpack(Cl)])
                end,
                Cl
        end,

    Clusters = pmap(fun(Cluster) -> StartDCs(Cluster) end, ClusterDcSetup),

    %% DCs started, but not connected yet
    pmap(
      fun([{Status, MainNode} | _] = CurrentCluster) ->
              case Status of
                  ready -> ok;
                  connect ->
                      ct:pal("~p of ~p subscribing to other external DCs",
                       [MainNode, unpack(CurrentCluster)]),

                      Descriptors =
                          lists:map(fun([{_Status, FirstNode} | _]) ->
                                            {ok, Descriptor} =
                                                rpc:call(FirstNode,
                                                         antidote_dc_manager,
                                                         get_connection_descriptor, []),
                                            Descriptor
                                    end, Clusters),

                      %% subscribe to descriptors of other dcs
                      ok = rpc:call(MainNode, antidote_dc_manager,
                                    subscribe_updates_from, [Descriptors])
              end
      end, Clusters),

    ct:log("Clusters joined and data centers connected connected: ~p", [ClusterDcSetup]),
    [unpack(DC) || DC <- Clusters].

-spec pmap(fun(), list()) -> list().
pmap(F, L) ->
    Parent = self(),
    lists:foldl(
        fun(X, N) ->
            spawn_link(fun() ->
                           Parent ! {pmap, N, F(X)}
                       end),
            N+1
        end, 0, L),
    L2 = [receive {pmap, N, R} -> {N, R} end || _ <- L],
    {_, L3} = lists:unzip(lists:keysort(1, L2)),
    L3.

-spec unpack([{ready | connect, atom()}]) -> [atom()].
unpack(NodesWithStatus) ->
    [Node || {_Status, Node} <- NodesWithStatus].

get_name(Node) ->
    [Node0, _Host] = string:split(atom_to_list(Node), "@"),
    list_to_atom(Node0).

an_connect() ->
    an_connect_int(0).
an_connect(Node) when is_atom(Node) ->
    an_connect_int(node_modifier(get_name(Node))).

an_connect_int(N) ->
    antidotec_pb_socket:start_link("127.0.0.1",
                                   vx_test_utils:port(antidote, pb_port, N)).

an_start_tx(Pid) ->
    antidotec_pb:start_transaction(Pid, ignore, [{static, false}]).

an_commit_tx(Pid, TxId) ->
    antidotec_pb:commit_transaction(Pid, TxId).

an_disconnect(Pid) ->
    antidotec_pb_socket:stop(Pid).

vx_connect() ->
    vx_connect_int(0).

vx_connect(Node) ->
    vx_connect_int(node_modifier(get_name(Node))).

vx_connect_int(N) ->
    vx_client:connect("127.0.0.1",
                                vx_test_utils:port(vx_server, pb_port, N), []).

vx_disconnect(C) ->
    vx_client:stop(C).

with_connection(Node, Fun) ->
    {ok, C} = vx_connect(Node),
    try Fun(C)
    after
        vx_disconnect(C)
    end.

with_replication(C, Opts, Fun) ->
    ok = vx_client:start_replication(C, Opts),
    ct:log("test replication started on: ~p~n", [C]),
    try Fun(C)
    after
        ct:log("test replication stopped on: ~p~n", [C]),
        vx_client:stop_replication(C)
    end.

with_replication_con(Node, Opts, Fun) ->
    with_connection(Node,
      fun(C) ->
              with_replication(C, Opts, Fun)
      end).

with_connection(Fun) ->
    {ok, C} = vx_connect(),
    try Fun(C)
    after
        vx_disconnect(C)
    end.

with_replication_con(Opts, Fun) ->
    with_connection(
      fun(C) ->
              with_replication(C, Opts, Fun)
      end).

assert_receive(Timeout) ->
    receive
        M ->
            erlang:error({unexpected_msg, M})
    after Timeout ->
            ok
    end.

assert_count(N, Timeout) ->
    assert_count(N, Timeout, fun(M) -> M end).

assert_count_msg(N, Timeout) ->
    assert_count(N, Timeout, fun(#vx_client_msg{msg = Msg}) ->
                                     {L, R} = Msg#vx_wal_txn.wal_offset,
                                     ?assert( ((0 < L) andalso (L < R)) orelse
                                              ((0 == L) andalso (L < R))
                                            ),
                                     Msg
                             end).

assert_count_msg(Source, N, Timeout) ->
    assert_count(N, Timeout, fun(#vx_client_msg{pid = Pid, msg = Msg})
                                   when Pid == Source ->
                                     {L, R} = Msg#vx_wal_txn.wal_offset,
                                     ?assert( ((0 < L) andalso (L < R)) orelse
                                              ((0 == L) andalso (L < R))
                                            ),
                                     Msg
                             end).


assert_count(0, _Timeout, _Fun) ->
    [];
assert_count(N, Timeout, Fun) ->
    receive
        M ->
            ct:log("received: ~p~n", [M]),
            [Fun(M) | assert_count(N-1, Timeout, Fun)]
    after Timeout ->
              erlang:error({not_sufficient_msg_count, N})
    end.
