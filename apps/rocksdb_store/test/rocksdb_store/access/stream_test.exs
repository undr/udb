defmodule UDB.RocksDBStore.StreamTest do
  use ExUnit.Case
  use UDB.Store.Access.StreamTestCase, module: UDB.RocksDBStore
end
