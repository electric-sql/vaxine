-module(vx_server_utils).

-export([ get_key_origin/1 ]).

-type key() :: binary().
-spec get_key_origin(key()) -> antidote:index_node().
get_key_origin(Key) ->
    Preflist = log_utilities:get_preflist_from_key(Key),
    hd(Preflist).
