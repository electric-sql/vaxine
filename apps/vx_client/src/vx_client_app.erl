%%%-------------------------------------------------------------------
%% @doc vx_client public API
%% @end
%%%-------------------------------------------------------------------

-module(vx_client_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    vx_client_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
