defmodule UDB.RocksDBStore.StreamImplTest do
  use ExUnit.Case
  alias UDB.RocksDBStore

  def stream(db, action, seek, opts \\ []) do
    Stream.resource(
      fn ->
        {:ok, iterator} = :rocksdb.iterator(db, opts)
        {seek, iterator}
      end,
      fn
        {^seek, iterator} ->
          case :rocksdb.iterator_move(iterator, seek) do
            {:ok, k, _v} -> {[k], iterator}
            _            -> {:halt, iterator}
          end
        iterator ->
          case :rocksdb.iterator_move(iterator, action) do
            {:ok, k, _v} -> {[k], iterator}
            _            -> {:halt, iterator}
          end
      end,
      fn iterator -> :rocksdb.iterator_close(iterator) end
    ) |> Enum.into([])
  end

  setup context do
    {:ok, db} = RocksDBStore.create("memdb", env: :memenv)

    seed_data(db, context)

    on_exit fn ->
      {:ok, _} = RocksDBStore.close(db)
    end

    {:ok, db: db.ref}
  end

  def seed_data(db, %{sparse: true}) do
    RocksDBStore.write(db, [
      {:put, "a2", "a2"},
      {:put, "a4", "a4"},
      {:put, "b2", "b2"},
      {:put, "b3", "b3"},
      {:put, "b5", "b5"},
      {:put, "c1", "c1"},
      {:put, "c3", "c3"},
      {:put, "c4", "c4"},
      {:put, "d2", "d2"},
      {:put, "d4", "d4"}
    ])
  end

  @tag sparse: true
  test "fwd range stream for sparse list", %{db: db} do
    # to last
    assert stream(db, :next, "c2", [iterate_lower_bound: "c2"]) ==
      ["c3", "c4", "d2", "d4"]
    # from first excluding last
    assert stream(db, :next, :first, [iterate_upper_bound: "c2"]) ==
      ["a2", "a4", "b2", "b3", "b5", "c1"]
    # excluding last
    assert stream(db, :next, "b1", [iterate_upper_bound: "c2"]) ==
      ["b2", "b3", "b5", "c1"]
    assert stream(db, :next, "b1", [iterate_lower_bound: "b1", iterate_upper_bound: "c2"]) ==
      ["b2", "b3", "b5", "c1"]
    # including last
    assert stream(db, :next, "b1", [iterate_upper_bound: "c2\xff"]) ==
      ["b2", "b3", "b5", "c1"]
    assert stream(db, :next, "b1", [iterate_lower_bound: "b1", iterate_upper_bound: "c2\xff"]) ==
      ["b2", "b3", "b5", "c1"]
  end

  @tag sparse: true
  test "fwd prefix stream for sparse list", %{db: db} do
    assert stream(db, :next, "b", [iterate_upper_bound: "b\xff"]) ==
      ["b2", "b3", "b5"]
    assert stream(db, :next, "b", [iterate_lower_bound: "b", iterate_upper_bound: "b\xff"]) ==
      ["b2", "b3", "b5"]
  end

  @tag sparse: true
  test "bwd range stream for sparse list", %{db: db} do
    # from last
    assert stream(db, :prev, :last, [iterate_lower_bound: "c2"]) ==
      ["d4", "d2", "c4", "c3"]
    # to first including last
    assert stream(db, :prev, {:seek_for_prev, "c2"}) ==
      ["c1", "b5", "b3", "b2", "a4", "a2"]
    # to first excluding last
    assert stream(db, :prev, {:seek_for_prev, "c2"}, [iterate_upper_bound: "c2"]) ==
      ["c1", "b5", "b3", "b2", "a4", "a2"]
    # including last
    assert stream(db, :prev, {:seek_for_prev, "c2"}, [iterate_lower_bound: "b2"]) ==
      ["c1", "b5", "b3", "b2"]
    # excluding last
    assert stream(db, :prev, {:seek_for_prev, "c2"}, [iterate_lower_bound: "b2", iterate_upper_bound: "c2"]) ==
      ["c1", "b5", "b3", "b2"]
  end

  @tag sparse: true
  test "bwd prefix stream for sparse list", %{db: db} do
    assert stream(db, :prev, {:seek_for_prev, "b\xff"}, [iterate_lower_bound: "b"]) ==
      ["b5", "b3", "b2"]
    assert stream(db, :prev, {:seek_for_prev, "b\xff"}, [iterate_lower_bound: "b", iterate_upper_bound: "b\xff"]) ==
      ["b5", "b3", "b2"]
  end

  def seed_data(db, %{}) do
    RocksDBStore.write(db, [
      {:put, "a1", "a1"},
      {:put, "a2", "a2"},
      {:put, "b1", "b1"},
      {:put, "b2", "b2"},
      {:put, "b3", "b3"},
      {:put, "c1", "c1"},
      {:put, "c2", "c2"},
      {:put, "c3", "c3"},
      {:put, "d1", "d1"},
      {:put, "d2", "d2"}
    ])
  end

  test "fwd range stream", %{db: db} do
    # to last
    # {:fwd, "c2"}
    assert stream(db, :next, "c2", [iterate_lower_bound: "c2"]) ==
      ["c2", "c3", "d1", "d2"]
    # from first excluding last
    # {:fwd_excl, {:first, "c2"}}
    assert stream(db, :next, :first, [iterate_upper_bound: "c2"]) ==
      ["a1", "a2", "b1", "b2", "b3", "c1"]
    # from first including last
    # {:fwd_incl, {:first, "c2"}}
    assert stream(db, :next, :first, [iterate_upper_bound: "c2\x00"]) ==
      ["a1", "a2", "b1", "b2", "b3", "c1", "c2"]
    # excluding last
    # {:fwd_excl, {"b2", "c2"}}
    assert stream(db, :next, "b2", [iterate_upper_bound: "c2"]) ==
      ["b2", "b3", "c1"]
    assert stream(db, :next, "b2", [iterate_lower_bound: "b2", iterate_upper_bound: "c2"]) ==
      ["b2", "b3", "c1"]
    # including last
    # {:fwd_incl, {"b2", "c2"}}
    assert stream(db, :next, "b2", [iterate_upper_bound: "c2\xff"]) ==
      ["b2", "b3", "c1", "c2"]
    assert stream(db, :next, "b2", [iterate_lower_bound: "b2", iterate_upper_bound: "c2\x00"]) ==
      ["b2", "b3", "c1", "c2"]
  end

  test "fwd prefix stream", %{db: db} do
    # {fwd_prefix, "b"}
    assert stream(db, :next, "b", [iterate_upper_bound: "b\xff"]) ==
      ["b1", "b2", "b3"]
    assert stream(db, :next, "b", [iterate_lower_bound: "b", iterate_upper_bound: "b\xff"]) ==
      ["b1", "b2", "b3"]
  end

  test "bwd range stream", %{db: db} do
    # from last
    # {:bwd, "c2"}
    assert stream(db, :prev, :last, [iterate_lower_bound: "c2"]) ==
      ["d2", "d1", "c3", "c2"]
    # to first including last
    # {:bwd_incl, {:first, "c2"}}
    assert stream(db, :prev, {:seek_for_prev, "c2"}) ==
      ["c2", "c1", "b3", "b2", "b1", "a2", "a1"]
    # to first excluding last
    # {:bwd_excl, {:first, "c2"}}
    assert stream(db, :prev, {:seek_for_prev, "c2"}, [iterate_upper_bound: "c2"]) ==
      ["c1", "b3", "b2", "b1", "a2", "a1"]
    # including last
    # {:bwd_incl, {"b2", "c2"}}
    assert stream(db, :prev, {:seek_for_prev, "c2"}, [iterate_lower_bound: "b2"]) ==
      ["c2", "c1", "b3", "b2"]
    # excluding last
    # {:bwd_excl, {"b2", "c2"}}
    assert stream(db, :prev, {:seek_for_prev, "c2"}, [iterate_lower_bound: "b2", iterate_upper_bound: "c2"]) ==
      ["c1", "b3", "b2"]
  end

  test "bwd prefix stream", %{db: db} do
    # {:bwd_prefix, "b"}
    assert stream(db, :prev, {:seek_for_prev, "b\xff"}, [iterate_lower_bound: "b"]) ==
      ["b3", "b2", "b1"]
    assert stream(db, :prev, {:seek_for_prev, "b\xff"}, [iterate_lower_bound: "b", iterate_upper_bound: "b\xff"]) ==
      ["b3", "b2", "b1"]
  end
end
