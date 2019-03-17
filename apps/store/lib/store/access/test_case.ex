defmodule UDB.Store.Access.TestCase do
  defmacro __using__(opts) do
    {:ok, module} = Keyword.fetch(opts, :module)

    quote location: :keep do
      alias UDB.Store

      describe "#{unquote(module)}" do
        use UDB.Store.TestCase, unquote(opts)

        def seed_data(conn, %{}) do
          ["a2", "a4", "b2", "b3", "b5", "c1", "c3", "c4", "d2", "d4"] |> Enum.into(conn)
        end

        test ".get", %{conn: conn} do
          assert Store.get(conn, "a4") == "a4"
          assert Store.get(conn, "a3") == nil
        end

        test ".put", %{conn: conn} do
          assert Store.put(conn, "key", "value") == :ok
          assert Store.get(conn, "key") == "value"
        end

        test ".delete", %{conn: conn} do
          assert Store.get(conn, "a2") == "a2"
          assert Store.delete(conn, "a2") == :ok
          assert Store.get(conn, "a2") == nil
          assert Store.delete(conn, "a2") == :ok
          assert Store.get(conn, "a2") == nil
        end
      end
    end
  end
end
