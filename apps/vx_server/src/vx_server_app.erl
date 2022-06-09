%%%-------------------------------------------------------------------
%% @doc vx_server public API
%% @end
%%%-------------------------------------------------------------------

-module(vx_server_app).
-behaviour(application).

-define(APP, vx_server).

-export([start/2, stop/1]).
-export([get_pb_pool_size/0,
         get_pb_max_connections/0,
         get_pb_port/0]).

start(_StartType, _StartArgs) ->
    vx_server_sup:start_link().

stop(_State) ->
    ok.

get_pb_pool_size() ->
    application:get_env(?APP, pb_pool_size, 100).

get_pb_max_connections() ->
    application:get_env(?APP, pb_max_connections, 1024).

get_pb_port() ->
    application:get_env(?APP, pb_port, 8088).

%% internal functions
