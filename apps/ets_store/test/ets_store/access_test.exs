defmodule UDB.ETSStore.AccessTest do
  use ExUnit.Case
  use UDB.Store.Access.TestCase, module: UDB.ETSStore, close: false
end
