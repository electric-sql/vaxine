-module(vx_client).

-export([ connect/3,
          disconnect/1,
          subscribe/4,
          unsubscribe/2,
          stop/1
        ]).

-export_type([sub_id/0]).

%% FIXME: Currently snapshot is ignored
-type snapshot() :: term().

-type sub_id() :: reference().
-type address() :: inet:socket_address() | inet:hostname().

-spec connect(address(), inet:port_number(), list()) ->
          {ok, pid()} | {error, term()}.
connect(Address, Port, Options) ->
    vx_client_socket:start_link(Address, Port, Options).

-spec disconnect(pid()) -> ok | {error, term()}.
disconnect(Pid) ->
    vx_client_socket:disconnect(Pid).

-spec subscribe(pid(), [binary()], snapshot(), boolean()) ->
          {ok, sub_id()} | {error, term()}.
subscribe(Pid, Keys, Snapshot, SnapshotFlag) ->
    vx_client_socket:subscribe(Pid, Keys, Snapshot, SnapshotFlag).

-spec unsubscribe(pid(), sub_id()) ->
          ok | {error, term()}.
unsubscribe(Pid, SubId) ->
    vx_client_socket:unsubscribe(Pid, SubId).

stop(Pid) ->
    vx_client_socket:stop(Pid).
