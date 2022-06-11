defmodule Vax.ConnectionPool.BackoffWorker do
  @moduledoc false

  use GenServer

  @backoff_reset_interval_ms 60_000
  @default_max_backoff_ms 5_000

  @type state :: %{
          last_failure: integer() | nil,
          last_backoff: integer() | nil,
          max_backoff: integer(),
          backoff_reset_interval: integer()
        }

  @spec start_link(Keyword.t()) :: GenServer.on_start()
  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :name)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @spec wait_for_backoff(pid() | atom()) :: :ok
  def wait_for_backoff(pid_or_name) do
    pid_or_name
    |> GenServer.call(:reconnection_backoff)
    |> :timer.sleep()
  end

  @spec wait_for_backoff(pid() | atom()) :: :ok
  def register_failure(pid_or_name) do
    GenServer.cast(pid_or_name, :register_failure)
  end

  @impl GenServer
  def init(opts) do
    max_backoff = Keyword.get(opts, :max_backoff, @default_max_backoff_ms)

    backoff_reset_interval =
      Keyword.get(opts, :backoff_reset_interval, @backoff_reset_interval_ms)

    {:ok,
     %{
       last_failure: nil,
       last_backoff: nil,
       max_backoff: max_backoff,
       backoff_reset_interval: backoff_reset_interval
     }}
  end

  @impl GenServer
  def handle_call(:reconnection_backoff, _from, state) do
    new_backoff = compute(state)
    {:reply, new_backoff, %{state | last_backoff: new_backoff}}
  end

  @impl GenServer
  def handle_cast(:register_failure, state) do
    {:noreply, %{state | last_failure: System.monotonic_time(:millisecond)}}
  end

  @spec compute(state()) :: integer()
  defp compute(%{last_failure: nil}) do
    0
  end

  defp compute(state) do
    if System.monotonic_time(:millisecond) - state.last_failure >= state.backoff_reset_interval do
      0
    else
      min(state.last_backoff + 500, state.max_backoff)
    end
  end
end
