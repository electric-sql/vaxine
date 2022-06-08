%%%-------------------------------------------------------------------
%% @doc vx_server public API
%% @end
%%%-------------------------------------------------------------------

-module(vx_server_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    vx_server_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
