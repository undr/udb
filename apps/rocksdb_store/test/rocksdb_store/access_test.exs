defmodule UDB.RocksDBStore.AccessTest do
  use ExUnit.Case
  use UDB.Store.Access.TestCase, module: UDB.RocksDBStore
end
