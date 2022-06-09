-module(vx_subs_server_SUITE).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(APP, vx_server).
-define(comment(A), [{userdata,
                      [{description, A}]
                     }]).

-define(rpc(Node, Block),
        rpc:call(Node, erlang, apply, [fun() -> Block end, []])).
-define(rpc_async(Node, Block),
        rpc:async_call(Node, erlang, apply, [fun() -> Block end, []])).
-define(rpc_await(Key), rpc:yield(Key)).

all() ->
    [ %%single_node_test,
      {group, setup_cluster}
    ].

groups() ->
    [{setup_cluster, [], [sanity_check]}].

init_per_suite(_, Config) ->
    Config.

end_per_suite(_, Config) ->
    Config.

init_per_group(setup_cluster, Config) ->
    init_testsuite(),
    Nodes = start_dc(3, Config),
    [{nodes, Nodes}, {cookie, erlang:get_cookie()}| Config].

end_per_group(setup_cluster, Config) ->
    NodesInfo = ?config(nodes, Config),
    [ct_slave:stop(Node) || {_, Node} <- NodesInfo].


init_node_meck() ->
    application:ensure_all_started(meck).

    %%meck:new(simple_crdt_store_memtable, [no_link, passthrough]).

reset_node() ->
    meck:unload().

%%------------------------------------------------------------------------------

sanity_check() ->
    ?comment("Sanity check for the cluster").
sanity_check(Config) ->
    Nodes = ?config(nodes, Config),

    {ok, _} = application:ensure_all_started(vx_client),

    lists:map(
      fun({N, _Node}) ->
              {ok, Pid} = vx_client:connect("localhost", port(?APP, pb_port, N), []),
              vx_client:stop(Pid)
      end, Nodes),
    ok.

port(antidote, logreader_port, N) -> port(N, 0);
port(antidote, pubsub_port, N)    -> port(N, 1);
port(antidote, pb_port, N)        -> port(N, 2);
port(antidote, handoff_port, N)   -> port(N, 3);
port(antidote, metric_port, N)    -> port(N, 4);
port(?APP,     pb_port, N)        -> port(N, 5).

port(PortModifier, SeqNum) ->
    10000 + PortModifier * 10 + SeqNum.

%%------------------------------------------------------------------------------

start_dc(NodesNum, Config) ->
    NodeNameFun = fun(N) ->
                          erlang:list_to_atom(
                            lists:flatten(
                              io_lib:format("node-~p",[N])))
                  end,
    Nodes = [{N, NodeNameFun(N)} || N <- lists:seq(1, NodesNum)],

    lists:foreach(fun({connect, _Node}) -> ok end,
                  rpc:pmap({?MODULE, start_node},
                           [Config], Nodes)),
    Nodes.


%% Copied from antidote/test/utils/test_utils
-spec start_node({integer(), atom()}, list()) ->
          {connect, node()} | {ready, node()}.
start_node({PortModifier, Name}, Config) ->
    %% have the slave nodes monitor the runner node, so they can't outlive it
    NodeConfig = [{monitor_master, true}],
    case ct_slave:start(Name, NodeConfig) of
        {ok, Node} ->
            %% code path for compiled dependencies
            CodePath = lists:filter(fun filelib:is_dir/1, code:get_path()) ,
            lists:foreach(fun(P) -> rpc:call(Node, code, add_patha, [P]) end, CodePath),

            % load application to allow for configuring the environment before starting
            ok = rpc:call(Node, application, load, [riak_core]),
            ok = rpc:call(Node, application, load, [antidote_stats]),
            ok = rpc:call(Node, application, load, [ranch]),
            ok = rpc:call(Node, application, load, [antidote]),
            ok = rpc:call(Node, application, load, [vx_server]),

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

            %% VX_SERVER Configuration
            {ok, _} = rpc:call(Node, application, ensure_all_started, [?APP]),
            ct:pal("Node ~p started with ports ~p-~p", [Node, Port, Port + 5]),

            {connect, Node};
        {error, already_started, Node} ->
            ct:log("Node ~p already started, reusing node", [Node]),
            {error, Node};
        {error, Reason, Node} ->
            ct:pal("Error starting node ~w, reason ~w, will retry", [Node, Reason]),
            ct_slave:stop(Name),
            %% FIXME: Probably make sense to reuse functionality from antidote as well
            %% time_utils:wait_until_offline(Node),
            timer:sleep(2000),
            start_node(Name, Config)
    end.

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
