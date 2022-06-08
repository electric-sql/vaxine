-module(vx_client).

-export([ connect/3,
          disconnect/1,
          start_replication/2,
          get_next_stream_bulk/2,
          stop_replication/1,
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

%% @doc Start WAL streaming replication. Currently no options are supported
%% And streaming would start from the beginning of the WAL file.
%%
%% Once the replication started the client of this API is expected to
%% receive #vx_client_msg messages:
-spec start_replication(pid(), list()) -> ok | {error, term()}.
start_replication(Pid, Options) ->
    vx_client_socket:start_replication(Pid, Options).

%% @doc Part of streaming API. Calling process notifies client connection that
%% it's ready to process another portion of transactions. If N=0 is provided
%% connection would not wait for message processing and will send it to the
%% calling process as fast as possible.
-spec get_next_stream_bulk(pid(), non_neg_integer()) -> ok | {error, term()}.
get_next_stream_bulk(Pid, N) ->
    vx_client_socket:get_next_stream_bulk(Pid, N).

%% @doc Stops replication
-spec stop_replication(pid()) -> ok | {error, term()}.
stop_replication(Pid) ->
    vx_client_socket:stop_replication(Pid).

stop(Pid) ->
    vx_client_socket:stop(Pid).
