defmodule UDB.Commands.HashTest do
  use ExUnit.Case

  alias UDB.RocksDBStore
  alias UDB.Commands.Hash

  setup context do
    {:ok, conn} = RocksDBStore.create("memdb", env: :memenv)

    seed_data(conn, context)

    on_exit fn ->
      {:ok, _} = RocksDBStore.close(conn)
    end

    {:ok, conn: conn}
  end

  def seed_data(_conn, %{empty: true}), do: :ok
  def seed_data(conn, %{}) do
    Hash.set(conn, "key", %{
      "fld1" => "val1",
      "fld2" => "val2",
      "fld3" => "val3",
      "fld4" => "val4",
      "fld5" => "val5",
      "fld6" => "val6"
    })
  end

  test "getall", %{conn: conn} do
    assert Hash.getall(conn, "key") == %{
      "fld1" => "val1",
      "fld2" => "val2",
      "fld3" => "val3",
      "fld4" => "val4",
      "fld5" => "val5",
      "fld6" => "val6"
    }
    assert Hash.getall(conn, "key2") == %{}
  end

  test "keys", %{conn: conn} do
    assert Hash.keys(conn, "key") == ["fld1", "fld2", "fld3", "fld4", "fld5", "fld6"]
    assert Hash.keys(conn, "key2") == []
  end

  test "values", %{conn: conn} do
    assert Hash.values(conn, "key") == ["val1", "val2", "val3", "val4", "val5", "val6"]
    assert Hash.values(conn, "key2") == []
  end

  test "get", %{conn: conn} do
    assert Hash.get(conn, "key", "fld1") == "val1"
    assert Hash.get(conn, "key", "fld0") == nil
    assert Hash.get(conn, "key2", "fld2") == nil
  end

  test "len", %{conn: conn} do
    assert Hash.len(conn, "key") == 6
    Hash.set(conn, "key", %{"fld7" => "val7", "fld8" => "val8"})
    assert Hash.len(conn, "key") == 8
    Hash.del(conn, "key", "fld3")
    assert Hash.len(conn, "key") == 7
    Hash.delall(conn, "key")
    assert Hash.len(conn, "key") == 0
    assert Hash.len(conn, "key2") == 0
  end

  test "delall", %{conn: conn} do
    assert Hash.len(conn, "key") == 6
    Hash.delall(conn, "key")
    assert Hash.len(conn, "key") == 0
    assert Hash.delall(conn, "key2") == :ok
  end

  test "del", %{conn: conn} do
    assert Hash.len(conn, "key") == 6
    Hash.del(conn, "key", "fld3")
    assert Hash.len(conn, "key") == 5
    assert Hash.getall(conn, "key") == %{
      "fld1" => "val1",
      "fld2" => "val2",
      "fld4" => "val4",
      "fld5" => "val5",
      "fld6" => "val6"
    }
    Hash.del(conn, "key", ["fld1", "fld5"])
    assert Hash.len(conn, "key") == 3
    assert Hash.getall(conn, "key") == %{"fld2" => "val2", "fld4" => "val4", "fld6" => "val6"}
    assert Hash.del(conn, "key", "fld0") == :ok
    assert Hash.len(conn, "key") == 3
    assert Hash.del(conn, "key2", "fld0") == :ok
  end

  @tag empty: true
  test "set", %{conn: conn} do
    assert Hash.set(conn, "key", {"fld1", "val1"}) == :ok
    assert Hash.getall(conn, "key") == %{"fld1" => "val1"}
    assert Hash.len(conn, "key") == 1

    assert Hash.set(conn, "key", [{"fld1", "val11"}, {"fld2", "val2"}]) == :ok
    assert Hash.getall(conn, "key") == %{"fld1" => "val11", "fld2" => "val2"}
    assert Hash.len(conn, "key") == 2

    assert Hash.set(conn, "key", %{"fld3" => "val3", "fld2" => "val22"}) == :ok
    assert Hash.getall(conn, "key") == %{"fld1" => "val11", "fld2" => "val22", "fld3" => "val3"}
    assert Hash.len(conn, "key") == 3

    assert Hash.set(conn, "key", {"fld3", "val33"}) == :ok
    assert Hash.getall(conn, "key") == %{"fld1" => "val11", "fld2" => "val22", "fld3" => "val33"}
    assert Hash.len(conn, "key") == 3
  end
end
