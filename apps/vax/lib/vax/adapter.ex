defmodule Vax.Adapter do
  @moduledoc """
  Ecto adapter for Vaxine
  """

  alias Vax.ConnectionPool
  alias Vax.Adapter.AntidoteClient
  alias Vax.Adapter.Query

  @bucket "vax"

  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Queryable
  @behaviour Ecto.Adapter.Storage
  @behaviour Ecto.Adapter.Transaction

  @impl Ecto.Adapter.Queryable
  def stream(_adapter_meta, _query_meta, _query_cache, _params, _options) do
    raise "Not implemented"
  end

  @impl Ecto.Adapter.Queryable
  def prepare(:update_all, _query), do: raise("Not implemented")
  def prepare(:delete_all, _query), do: raise("Not implemented")

  def prepare(:all, query) do
    {:nocache, query}
  end

  @impl Ecto.Adapter.Queryable
  def execute(adapter_meta, query_meta, {:nocache, query}, params, _options) do
    objs = Query.query_to_objs(query, params, @bucket)
    fields = Query.select_fields(query_meta)
    schema = Query.select_schema(query)
    defaults = struct(schema)

    adapter_meta.repo.transaction(
      fn ->
        {conn, tx_id} = get_transaction_data()
        {:ok, results} = AntidoteClient.read_objects(conn, objs, tx_id)

        results =
          for result <- results,
              {:antidote_map, values, _adds, _removes} = result,
              values != %{} do
            Enum.map(fields, fn field ->
              field_type = schema.__schema__(:type, field)
              field_default = Map.get(defaults, field)

              Vax.Adapter.Helpers.get_antidote_map_field_or_default(
                result,
                Atom.to_string(field),
                field_type,
                field_default
              )
            end)
          end

        {Enum.count(results), results}
      end,
      static: true
    )
  end

  @impl Ecto.Adapter
  def loaders(_primitive_type, :string), do: [&Vax.Type.client_load/1, :string]
  def loaders(_primitive_type, :binary_id), do: [&Vax.Type.client_load/1, :binary_id]

  def loaders(_primitive_type, ecto_type) do
    if Vax.Type.base_or_composite?(ecto_type) do
      [&Vax.Type.client_load/1, &binary_to_term/1, ecto_type]
    else
      [ecto_type]
    end
  end

  @impl Ecto.Adapter
  def dumpers(:binary_id, type), do: [type]
  def dumpers(:string, :string), do: [:string]
  def dumpers({:in, _primitive_type}, {:in, ecto_type}), do: [&dump_inner(&1, ecto_type)]
  def dumpers(_primitive_type, ecto_type), do: [ecto_type, &term_to_binary/1]

  defp term_to_binary(term), do: {:ok, :erlang.term_to_binary(term)}
  defp binary_to_term(nil), do: {:ok, nil}
  defp binary_to_term([]), do: {:ok, nil}
  defp binary_to_term(binary), do: {:ok, :erlang.binary_to_term(binary)}

  defp dump_inner(values, ecto_type) do
    values
    |> Enum.reduce_while([], fn v, acc ->
      case Ecto.Type.adapter_dump(__MODULE__, ecto_type, v) do
        {:ok, v} -> {:cont, [v | acc]}
        :error -> {:halt, :error}
      end
    end)
    |> case do
      :error -> :error
      list -> {:ok, Enum.reverse(list)}
    end
  end

  @impl Ecto.Adapter
  def init(config) do
    log? = Keyword.get(config, :log, true)
    if log?, do: Vax.Adapter.Logger.attach()

    opts =
      config
      |> Keyword.update!(:hostname, &String.to_charlist/1)
      |> Keyword.put_new(:port, 8087)
      |> Keyword.put_new(:pool_size, 10)

    {:ok, ConnectionPool.child_spec(opts), %{}}
  end

  @impl Ecto.Adapter
  def ensure_all_started(_config, _type) do
    {:ok, []}
  end

  @impl Ecto.Adapter
  def checkout(%{repo: repo}, _config, function) do
    if Process.get(:vax_checked_out_conn) do
      function.()
    else
      pool = ConnectionPool.pool_name(repo)

      ConnectionPool.checkout(pool, fn {_pid, _ref}, pid ->
        try do
          Process.put(:vax_checked_out_conn, pid)
          result = function.()

          {result, pid}
        after
          Process.put(:vax_checked_out_conn, nil)
        end
      end)
    end
  end

  @impl Ecto.Adapter
  def checked_out?(_adapter_meta) do
    not is_nil(Process.get(:vax_checked_out_conn))
  end

  @impl Ecto.Adapter
  defmacro __before_compile__(_env) do
    quote do
      @doc """
      Increments a counter

      See `Vax.Adapter.increment_counter/3` for more information
      """
      @spec increment_counter(key :: binary(), amount :: integer()) :: :ok
      def increment_counter(key, amount) do
        Vax.Adapter.increment_counter(__MODULE__, key, amount)
      end

      @doc """
      Reads a counter

      See `Vax.Adapter.read_counter/2` for more information
      """
      @spec read_counter(key :: binary()) :: integer()
      def read_counter(key) do
        Vax.Adapter.read_counter(__MODULE__, key)
      end

      def insert(changeset_or_struct, opts \\ []) do
        Vax.Adapter.Schema.insert(__MODULE__, changeset_or_struct, opts)
      end

      def update(changeset, opts \\ []) do
        Vax.Adapter.Schema.update(__MODULE__, changeset, opts)
      end

      def insert_or_update(changeset, opts \\ []) do
        Vax.Adapter.Schema.insert_or_update(__MODULE__, changeset, opts)
      end

      def delete(schema, opts \\ []) do
        Vax.Adapter.Schema.delete(__MODULE__, schema, opts)
      end

      def insert!(changeset_or_struct, opts \\ []) do
        Vax.Adapter.Schema.insert!(__MODULE__, changeset_or_struct, opts)
      end

      def update!(changeset, opts \\ []) do
        Vax.Adapter.Schema.update!(__MODULE__, changeset, opts)
      end

      def insert_or_update!(changeset, opts \\ []) do
        Vax.Adapter.Schema.insert_or_update!(__MODULE__, changeset, opts)
      end

      def delete!(schema, opts \\ []) do
        Vax.Adapter.Schema.delete!(__MODULE__, schema, opts)
      end
    end
  end

  @impl Ecto.Adapter.Storage
  def storage_up(_) do
    :ok
  end

  @impl Ecto.Adapter.Storage
  def storage_down(_) do
    :ok
  end

  @impl Ecto.Adapter.Storage
  def storage_status(_) do
    :up
  end

  @impl Ecto.Adapter.Transaction
  def in_transaction?(_adapter_meta) do
    not is_nil(Process.get(:vax_tx_id))
  end

  @impl Ecto.Adapter.Transaction
  def rollback(_adapter_meta, _value) do
    raise "Not implemented"
  end

  @impl Ecto.Adapter.Transaction
  def transaction(adapter_meta, opts, fun) do
    checkout(adapter_meta, [], fn ->
      conn = get_conn()

      if in_transaction?(adapter_meta) do
        fun.()
      else
        try do
          {:ok, tx_id} = start_or_continue_transaction(conn, opts[:static] || false)

          if opts[:static] do
            fun.()
          else
            result = fun.()

            case AntidoteClient.commit_transaction(conn, tx_id) do
              {:ok, _tx_id} ->
                result

              {:error, _e} = error ->
                error
            end
          end
        after
          Process.put(:vax_tx_id, nil)
        end
      end
    end)
  end

  @doc """
  Reads a counter
  """
  @spec read_counter(repo :: atom() | pid(), key :: binary()) :: integer()
  def read_counter(repo, key) do
    repo.transaction(
      fn ->
        {conn, tx_id} = get_transaction_data()
        obj = {key, :antidote_crdt_counter_pn, @bucket}
        {:ok, [result]} = AntidoteClient.read_objects(conn, [obj], tx_id)

        :antidotec_counter.value(result)
      end,
      static: true
    )
  end

  @doc """
  Increases a counter
  """
  @spec increment_counter(repo :: atom() | pid(), key :: binary(), amount :: integer()) :: :ok
  def increment_counter(repo, key, amount) do
    repo.transaction(
      fn ->
        {conn, tx_id} = get_transaction_data()
        obj = {key, :antidote_crdt_counter_pn, @bucket}
        counter = :antidotec_counter.increment(amount, :antidotec_counter.new())
        counter_update_ops = :antidotec_counter.to_ops(obj, counter)

        AntidoteClient.update_objects(conn, counter_update_ops, tx_id)
      end,
      static: true
    )
  end

  defp get_conn(), do: Process.get(:vax_checked_out_conn) || raise("Missing connection")

  def get_transaction_data do
    {Process.get(:vax_checked_out_conn), Process.get(:vax_tx_id)}
  end

  defp start_or_continue_transaction(conn, static?) do
    case Process.get(:vax_tx_id) do
      nil ->
        {:ok, tx_id} =
          if static? do
            AntidoteClient.start_transaction(conn, :ignore, static: true)
          else
            AntidoteClient.start_transaction(conn, :ignore)
          end

        Process.put(:vax_tx_id, tx_id)
        {:ok, tx_id}

      tx_id ->
        {:ok, tx_id}
    end
  end
end
