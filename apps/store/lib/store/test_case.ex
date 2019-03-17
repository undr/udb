defmodule UDB.Store.TestCase do
  defmacro __using__(opts) do
    {:ok, module} = Keyword.fetch(opts, :module)
    close = Keyword.get(opts, :close, true)

    quote location: :keep do
      setup context do
        {:ok, conn} = UDB.Store.create(unquote(module), "memdb1", env: :memenv)
        {:ok, empty} = UDB.Store.create(unquote(module), "memdb2", env: :memenv)

        seed_data(conn, context)

        on_exit fn ->
          if unquote(close) do
            {:ok, _} = UDB.Store.close(conn)
            {:ok, _} = UDB.Store.close(empty)
          end
        end

        {:ok, conn: conn, empty: empty, store: unquote(module)}
      end
    end
  end
end
