defmodule Vax.Types.Map do
  @moduledoc """
  Type for CRDT maps. All keys are strings, all values are LWW registers.
  """

  use Vax.ParameterizedType

  @impl Ecto.ParameterizedType
  def type(_), do: :map

  @impl Ecto.ParameterizedType
  def init(params) do
    Keyword.get(params, :strategy, :observed_remove)
  end

  @impl Ecto.ParameterizedType
  def load(nil, _loader, _params) do
    {:ok, %{}}
  end

  def load(value, _loader, _params) do
    loaded =
      value
      |> :antidotec_map.value()
      |> Map.new(fn {{k, _type}, v} -> {k, :erlang.binary_to_term(v)} end)

    {:ok, loaded}
  end

  @impl Ecto.ParameterizedType
  def dump(nil, _loader, _params) do
    {:ok, %{}}
  end

  def dump(value, dumper, _params) do
    Ecto.Type.dump(:map, value, dumper)
  end

  @impl Ecto.ParameterizedType
  def cast(data, _params) do
    Ecto.Type.cast(:map, data)
  end

  @impl Vax.ParameterizedType
  def compute_change(_params, antidotec_map, map) do
    map = Map.new(map, fn {k, v} -> {to_string(k), v} end)

    base_keys =
      antidotec_map
      |> :antidotec_map.value()
      |> Enum.map(fn {{k, _t}, _v} -> k end)

    removes = base_keys -- Map.keys(map)

    antidotec_map =
      map
      |> Enum.reduce(antidotec_map, fn {k, v}, antidotec_map ->
        key = {k, :antidote_crdt_register_lww}

        old_value =
          antidotec_map
          |> :antidotec_map.value()
          |> Map.get(key)
          |> case do
            nil -> :antidotec_reg.new()
            value -> :antidotec_reg.new(value)
          end

        value = :antidotec_reg.assign(old_value, :erlang.term_to_binary(v))
        :antidotec_map.add_or_update(antidotec_map, key, value)
      end)

    Enum.reduce(removes, antidotec_map, fn key, antidotec_map ->
      :antidotec_map.remove(antidotec_map, key)
    end)
  end

  @impl Vax.ParameterizedType
  def antidote_crdt_type(:observed_remove), do: :antidote_crdt_map_rr
  def antidote_crdt_type(:grow_only), do: :antidote_crdt_map_go

  @impl Vax.ParameterizedType
  def client_dump(_params, nil) do
    :antidotec_map.new()
  end

  def client_dump(_params, enumerable) do
    enumerable
    |> Enum.map(fn {k, v} ->
      {{to_string(k), :antidote_crdt_register_lww}, :erlang.term_to_binary(v)}
    end)
    |> :antidotec_map.new()
  end
end
