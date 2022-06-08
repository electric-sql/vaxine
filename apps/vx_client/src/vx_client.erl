-module(vx_client).

-export([ connect/3,
          disconnect/1,
          subscribe/5,
          unsubscribe/2
        ]).

%% FIXME: Currently snapshot is ignored
-type snapshot() :: term().

-type sub_id() :: reference().
-type address() :: inet:socket_address() | inet:hostname().

-spec connect(address(), inet:port_number(), list()) ->
          {ok, pid()} | {error, term()}.
connect(Address, Port, Options) ->
    vx_client_socket:start_link(Address, Port, Options).

-spec disconnect(pid()) -> ok.
disconnect(Pid) ->
    vx_client_socket:disconnect(Pid).

-spec subscribe(pid(), pid(), [binary()], snapshot(), boolean()) ->
          {ok, sub_id()} | {error, term()}.
subscribe(Pid, SubPid, Keys, _Snapshot, SnapshotFlag) ->
    vx_client_socket:subscribe(Pid, SubPid, Keys, SnapshotFlag).

-spec unsubscribe(pid(), sub_id()) ->
          ok | {error, term()}.
unsubscribe(Pid, SubId) ->
    vx_client_socket:unsubscribe(Pid, SubId).
