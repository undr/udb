defmodule UDB.Commands.ListTest do
  use ExUnit.Case
  use UDB.Commands.List.TestCase, module: UDB.ETSStore, close: false
  use UDB.Commands.List.TestCase, module: UDB.RocksDBStore
end
