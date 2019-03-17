defmodule UDB.RocksDBStore.AtomicTest do
  use ExUnit.Case
  use UDB.Store.Atomic.TestCase, module: UDB.RocksDBStore
end
