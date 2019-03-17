defmodule UDB.ETSStore.Access.StreamTest do
  use ExUnit.Case
  use UDB.Store.Access.StreamTestCase, module: UDB.ETSStore, close: false
end
