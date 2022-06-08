%%==============================================================================
%% @doc Main supervisor of the Vaxine-server application

-module(vx_server_sup).
-behaviour(supervisor).

-export([start_link/0,
        init/1]).

-define(CHILD(I, Type, Args),
        {I, {I, start_link, Args}, permanent, 5000, Type, [I]}).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%==============================================================================
%% Supervisor callbacks
%%==============================================================================

init([]) ->
    SupFlags = #{strategy => rest_for_one,
                 intensity => 1,
                 period => 5
                },
    ChildSpecs =
        [ ?CHILD(vx_subs_server, worker, []),
          ?CHILD(vx_proxy_server, worker, []),
          pb_sub_listener()
        ],
    {ok, {SupFlags, ChildSpecs}}.

pb_sub_listener() ->
    NumOfAcceptors = application:get_env(vaxine, pb_pool_size, 100),
    MaxConnections = application:get_env(vaxine, pb_max_connections, 1024),
    Port = application:get_env(vaxine, pb_port, 8088),

    ranch:child_spec(
      {?MODULE, vx_subs_worker}, ranch_tcp,
      #{ num_acceptors => NumOfAcceptors,
         max_connections => MaxConnections,
         socket_opts => [{port, Port}]
       },
      vx_subs_worker, []
     ).
