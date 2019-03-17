defmodule UDB.Store.Atomic.TestCase do
  defmacro __using__(opts) do
    {:ok, module} = Keyword.fetch(opts, :module)

    quote location: :keep do
      alias UDB.Store

      describe "#{unquote(module)}" do
        use UDB.Store.TestCase, unquote(opts)

        def seed_data(conn, %{}) do
        end

        test "atomic", %{conn: conn} do
        end
      end
    end
  end
end
