defmodule UDB.ETSStore.AtomicTest do
  use ExUnit.Case

  test "atomic is not implemented" do
    {:ok, conn} = UDB.Store.create(UDB.ETSStore, :memdb)
    refute UDB.Store.support_atomic?(conn)
    assert UDB.Store.atomic(conn, fn _ -> nil end) == {:error, :not_implemented}
  end
end
