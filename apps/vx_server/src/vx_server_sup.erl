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
        [ pb_sub_listener(),
          ?CHILD(vx_wal_stream_server, worker, [])
        ],
    {ok, {SupFlags, ChildSpecs}}.

pb_sub_listener() ->
    ranch:child_spec(
      {?MODULE, vx_wal_tcp_worker}, ranch_tcp,
      #{ num_acceptors => vx_server_app:get_pb_pool_size(),
         max_connections => vx_server_app:get_pb_max_connections(),
         socket_opts => [{port, vx_server_app:get_pb_port()}]
       },
      vx_wal_tcp_worker, []
     ).

