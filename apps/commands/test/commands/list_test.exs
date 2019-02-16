defmodule UDB.Commands.ListTest do
  use ExUnit.Case

  alias UDB.RocksDBStore
  alias UDB.Commands.List

  setup context do
    {:ok, conn} = RocksDBStore.create("memdb", env: :memenv)

    seed_data(conn, context)

    on_exit fn ->
      {:ok, _} = RocksDBStore.close(conn)
    end

    {:ok, conn: conn}
  end

  def seed_data(_conn, %{empty: true}) do
  end

  def seed_data(conn, %{}) do
    List.lpush(conn, "key", ["vall0", "vall1", "vall2"])
    List.rpush(conn, "key", ["valr0", "valr1", "valr2"])
  end

  test "all", %{conn: conn} do
    assert List.all(conn, "key") == ["vall2", "vall1", "vall0", "valr0", "valr1", "valr2"]
    assert List.all(conn, "key2") == []
  end

  test "index", %{conn: conn} do
    assert List.index(conn, "key", 0) == "vall2"
    assert List.index(conn, "key", -1) == "valr2"
    assert List.index(conn, "key", 3) == "valr0"
    assert List.index(conn, "key", 10) == nil
    assert List.index(conn, "key", -10) == nil
    assert List.index(conn, "key2", 0) == nil
  end

  test "range", %{conn: conn} do
    assert List.range(conn, "key", 0, -1) == ["vall2", "vall1", "vall0", "valr0", "valr1", "valr2"]
    assert List.range(conn, "key", 0, 2) == ["vall2", "vall1", "vall0"]
    assert List.range(conn, "key", 2, 2) == ["vall0"]
    assert List.range(conn, "key", 2, 4) == ["vall0", "valr0", "valr1"]
    assert List.range(conn, "key", 2, -2) == ["vall0", "valr0", "valr1"]
    assert List.range(conn, "key", 2, -5) == []
    assert List.range(conn, "key", -3, -1) == ["valr0", "valr1", "valr2"]
    assert List.range(conn, "key", -3, -10) == []
    assert List.range(conn, "key2", 2, 2) == []
  end

  test "set with positive index", %{conn: conn} do
    assert List.set(conn, "key", 0, "vall2-ed") == :ok
    assert List.all(conn, "key") == ["vall2-ed", "vall1", "vall0", "valr0", "valr1", "valr2"]
    assert List.set(conn, "key", 5, "valr2-ed") == :ok
    assert List.all(conn, "key") == ["vall2-ed", "vall1", "vall0", "valr0", "valr1", "valr2-ed"]
    assert List.set(conn, "key", 3, "valr0-ed") == :ok
    assert List.all(conn, "key") == ["vall2-ed", "vall1", "vall0", "valr0-ed", "valr1", "valr2-ed"]
  end

  test "set with negative index", %{conn: conn} do
    assert List.set(conn, "key", -6, "vall2-ed") == :ok
    assert List.all(conn, "key") == ["vall2-ed", "vall1", "vall0", "valr0", "valr1", "valr2"]
    assert List.set(conn, "key", -1, "valr2-ed") == :ok
    assert List.all(conn, "key") == ["vall2-ed", "vall1", "vall0", "valr0", "valr1", "valr2-ed"]
    assert List.set(conn, "key", -3, "valr0-ed") == :ok
    assert List.all(conn, "key") == ["vall2-ed", "vall1", "vall0", "valr0-ed", "valr1", "valr2-ed"]
  end

  test "set with invalid index", %{conn: conn} do
    assert List.set(conn, "key", 16, "vall2-ed") == {:error, {:index, :out_of_range}}
    assert List.set(conn, "key", -16, "valr2-ed") == {:error, {:index, :out_of_range}}
    assert List.set(conn, "key2", 0, "valr0-ed") == {:error, {:index, :out_of_range}}
    assert List.all(conn, "key2") == []
  end

  test "trim full range", %{conn: conn} do
    assert List.trim(conn, "key", 0, 5) == :ok
    assert List.all(conn, "key") == ["vall2", "vall1", "vall0", "valr0", "valr1", "valr2"]
  end

  test "trim tail", %{conn: conn} do
    assert List.trim(conn, "key", 0, 3) == :ok
    assert List.all(conn, "key") == ["vall2", "vall1", "vall0", "valr0"]
    assert List.trim(conn, "key", 0, -2) == :ok
    assert List.all(conn, "key") == ["vall2", "vall1", "vall0"]
  end

  test "trim head", %{conn: conn} do
    assert List.trim(conn, "key", 2, 5) == :ok
    assert List.all(conn, "key") == ["vall0", "valr0", "valr1", "valr2"]
    assert List.trim(conn, "key", 2, -1) == :ok
    assert List.all(conn, "key") == ["valr1", "valr2"]
  end

  test "trim range with positive and positive indexes", %{conn: conn} do
    assert List.trim(conn, "key", 2, 4) == :ok
    assert List.all(conn, "key") == ["vall0", "valr0", "valr1"]
  end

  test "trim range with positive and negative indexes", %{conn: conn} do
    assert List.trim(conn, "key", 2, -2) == :ok
    assert List.all(conn, "key") == ["vall0", "valr0", "valr1"]
  end

  test "trim range with negative and positive indexes", %{conn: conn} do
    assert List.trim(conn, "key", -4, 4) == :ok
    assert List.all(conn, "key") == ["vall0", "valr0", "valr1"]
  end

  test "trim range with negative and negative indexes", %{conn: conn} do
    assert List.trim(conn, "key", -4, -2) == :ok
    assert List.all(conn, "key") == ["vall0", "valr0", "valr1"]
  end

  test "trim range with the same positive indexes", %{conn: conn} do
    assert List.trim(conn, "key", 2, 2) == :ok
    assert List.all(conn, "key") == ["vall0"]
  end

  test "trim range with the same negative indexes", %{conn: conn} do
    assert List.trim(conn, "key", -2, -2) == :ok
    assert List.all(conn, "key") == ["valr1"]
  end

  test "trim range with invalid range", %{conn: conn} do
    assert List.trim(conn, "key", 4, 2) == :ok
    assert List.all(conn, "key") == []
  end

  @tag empty: true
  test "rpush", %{conn: conn} do
    assert List.rpush(conn, "key", ["valr1", "valr2"]) == 2
    assert List.all(conn, "key") == ["valr1", "valr2"]
    assert List.rpush(conn, "key", "valr3") == 3
    assert List.all(conn, "key") == ["valr1", "valr2", "valr3"]
  end

  @tag empty: true
  test "lpush", %{conn: conn} do
    assert List.lpush(conn, "key", ["vall1", "vall2"]) == 2
    assert List.all(conn, "key") == ["vall2", "vall1"]
    assert List.lpush(conn, "key", "vall3") == 3
    assert List.all(conn, "key") == ["vall3", "vall2", "vall1"]
  end

  @tag empty: true
  test "len", %{conn: conn} do
    assert List.len(conn, "key") == 0
    assert List.lpush(conn, "key", ["vall1", "vall2"]) == 2
    assert List.len(conn, "key") == 2
    assert List.lpush(conn, "key", "vall3") == 3
    assert List.len(conn, "key") == 3
  end
end
