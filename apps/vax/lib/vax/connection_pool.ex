defmodule Vax.ConnectionPool do
  @behaviour NimblePool

  alias Vax.ConnectionPool.BackoffWorker

  @spec checkout(pid() | atom(), ({pid(), reference()}, pid() -> {term(), pid()})) :: term()
  def checkout(pool, fun, _opts \\ []) do
    NimblePool.checkout!(pool, :checkout, fun)
  end

  @spec child_spec(Keyword.t()) :: Supervisor.child_spec()
  def child_spec(opts) do
    repo = Keyword.fetch!(opts, :repo)

    children = [
      %{
        id: BackoffWorker,
        start: {BackoffWorker, :start_link, [Keyword.put(opts, :name, backoff_worker_name(repo))]}
      },
      DynamicSupervisor.child_spec(
        name: connection_supervisor_name(repo),
        strategy: :one_for_one
      ),
      %{
        id: __MODULE__,
        start:
          {NimblePool, :start_link,
           [
             [
               name: pool_name(repo),
               worker: {__MODULE__, [hostname: opts[:hostname], port: opts[:port], repo: repo]},
               pool_size: opts[:pool_size] || 10
             ]
           ]}
      }
    ]

    %{
      id: ConnectionPoolSupervisor,
      start: {Supervisor, :start_link, [children, [strategy: :one_for_one]]}
    }
  end

  @impl true
  def init_worker(pool_state) do
    hostname = Keyword.fetch!(pool_state, :hostname)
    port = Keyword.fetch!(pool_state, :port)
    repo = Keyword.fetch!(pool_state, :repo)

    # Starting the protobufWorker socket is blocking, so we use NimblePool async
    # initialization. Before starting, backoff worker is requested to check whether
    # sleeping is needed.
    init_fun = fn ->
      backoff_worker = backoff_worker_name(repo)
      connection_supervisor = connection_supervisor_name(repo)
      BackoffWorker.wait_for_backoff(backoff_worker)

      pb_socket_child_spec = %{
        id: __MODULE__,
        start: {:antidotec_pb_socket, :start_link, [hostname, port]},
        restart: :temporary
      }

      case DynamicSupervisor.start_child(connection_supervisor, pb_socket_child_spec) do
        {:ok, socket_pid} ->
          %{pb_socket: socket_pid}

        error ->
          BackoffWorker.register_failure(backoff_worker)
          raise "Failed to connect to antidote with error #{inspect(error)}"
      end
    end

    {:async, init_fun, pool_state}
  end

  @impl true
  def handle_checkout(_maybe_wrapped_command, _from, worker_state, pool_state) do
    if Process.alive?(worker_state.pb_socket) do
      {:ok, worker_state.pb_socket, worker_state, pool_state}
    else
      {:remove, :dead, pool_state}
    end
  end

  @spec backoff_worker_name(atom()) :: atom()
  defp backoff_worker_name(repo) do
    Module.concat([repo, BackoffWorker])
  end

  @spec connection_supervisor_name(atom()) :: atom()
  defp connection_supervisor_name(repo) do
    Module.concat([repo, ConnectionSupervisor])
  end

  @spec pool_name(atom()) :: atom()
  def pool_name(repo) do
    Module.concat([repo, __MODULE__])
  end
end
