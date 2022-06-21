defmodule Vax.Types.MapTest do
  use ExUnit.Case, async: true

  alias Vax.Types.Map

  @encoded_1 :erlang.term_to_binary(1)

  setup_all do
    {:ok, map_type: Ecto.ParameterizedType.init(Map, [])}
  end

  describe "cast/2" do
    test "properly casts maps", %{map_type: map_type} do
      assert {:ok, %{"foo" => 1}} == Ecto.Type.cast(map_type, %{"foo" => 1})
      assert {:ok, %{foo: 1}} == Ecto.Type.cast(map_type, %{foo: 1})
      assert :error == Ecto.Type.cast(map_type, :foo)
    end
  end

  describe "compute_changes/3" do
    test "properly adds keys to map", %{map_type: map_type} do
      base_map = Vax.Type.client_dump(map_type, [])

      assert {:antidote_map, _value,
              %{{"foo", :antidote_crdt_register_lww} => {:antidote_reg, [], @encoded_1}},
              _removes} = Vax.Type.compute_change(map_type, base_map, %{"foo" => 1})
    end

    test "can handle maps with non-string keys", %{map_type: map_type} do
      # required because changeset allows other kinds of keys to be given as input
      base_map = Vax.Type.client_dump(map_type, [])

      assert {:antidote_map, _value,
              %{{"foo", :antidote_crdt_register_lww} => {:antidote_reg, [], @encoded_1}},
              _removes} = Vax.Type.compute_change(map_type, base_map, %{foo: 1})
    end

    test "properly removes items from map", %{map_type: map_type} do
      base_map = Vax.Type.client_dump(map_type, %{"foo" => 1})

      assert {:antidote_map, _value, _adds, ["foo"]} =
               Vax.Type.compute_change(map_type, base_map, %{bar: 2})

      assert {:antidote_map, _value, _adds, []} =
               Vax.Type.compute_change(map_type, base_map, %{foo: 2})
    end
  end
end
