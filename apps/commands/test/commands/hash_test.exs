defmodule UDB.Commands.HashTest do
  use ExUnit.Case
  use UDB.Commands.Hash.TestCase, module: UDB.ETSStore, close: false
  use UDB.Commands.Hash.TestCase, module: UDB.RocksDBStore
end
