-module(vx_test_utils).

-export([init_clean_node/2,
         init_clean_node/3,
         init_testsuite/0,
         port/3
        ]).

-define(APP, vx_server).

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
