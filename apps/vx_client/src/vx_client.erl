%%------------------------------------------------------------------------------
%% @doc Main API module for vx_client application
%%
%%------------------------------------------------------------------------------

-module(vx_client).

-export([ connect/3,
          start_replication/2,
          get_next_stream_bulk/2,
          stop_replication/1,
          stop/1
        ]).

-type wal_offset() :: term() | eof | 0.
-type connection() :: pid().
-type address() :: inet:socket_address() | inet:hostname().

-spec connect(address(), inet:port_number(), list()) ->
          {ok, connection()} | {error, term()}.
connect(Address, Port, Options) ->
    vx_client_socket:start_link(Address, Port, Options).

-spec stop(connection()) -> ok | {error, term()}.
stop(Pid) ->
    vx_client_socket:stop(Pid).

%% @doc Start WAL streaming replication.
%% Replication can be started either from the position provided by
%% the API previously (considered opaque) or from `eof` or `0` values.
%%
%% Once the replication started the client of this API is expected to
%% receive #vx_client_msg messages
-spec start_replication(connection(), [{offset, wal_offset()} |
                                       {sync, integer()}
                                      ]) ->
          ok | {error, term()}.
start_replication(Pid, Options) ->
    vx_client_socket:start_replication(Pid, Options).

%% @doc Part of streaming API. Calling process notifies client connection that
%% it's ready to process another portion of transactions. If N=0 is provided
%% connection would not wait for message processing and will send it to the
%% calling process as fast as possible.
-spec get_next_stream_bulk(connection(), non_neg_integer()) ->
          ok | {error, term()}.
get_next_stream_bulk(Pid, N) when is_integer(N) ->
    vx_client_socket:get_next_stream_bulk(Pid, N).

%% @doc Stops replication
-spec stop_replication(connection()) -> ok | {error, term()}.
stop_replication(Pid) ->
    vx_client_socket:stop_replication(Pid).
