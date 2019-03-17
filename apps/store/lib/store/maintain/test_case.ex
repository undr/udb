defmodule UDB.Store.Maintain.TestCase do
  defmacro __using__(opts) do
    {:ok, module} = Keyword.fetch(opts, :module)

    quote location: :keep do
      alias UDB.Store

      describe "#{unquote(module)}" do
        use UDB.Store.TestCase, unquote(opts)

        def seed_data(conn, %{}) do
          ["a2", "a4", "b2", "b3", "b5", "c1", "c3", "c4", "d2", "d4"] |> Enum.into(conn)
        end

        test ".is_empty?", %{conn: conn, empty: empty} do
          refute Store.is_empty?(conn)
          assert Store.is_empty?(empty)
        end

        test ".close", %{store: store} do
          {:ok, conn} = Store.create(store, "memdb3", env: :memenv)
          assert {:ok, closed_conn} = Store.close(conn)
          assert closed_conn.state == :closed
          assert {:error, :already_closed} = Store.close(conn)
          assert {:error, :already_closed} = Store.close(closed_conn)
        end

        test ".destroy", %{store: store} do
          {:ok, conn} = Store.create(store, "memdb3", env: :memenv)
          assert {:ok, destroyed_conn} = Store.destroy(conn)
          assert destroyed_conn.state == :destroyed
          assert {:error, :already_destroyed} = Store.destroy(conn)
          assert {:error, :already_destroyed} = Store.destroy(destroyed_conn)
        end

        test ".repair", %{store: store} do
        end
      end
    end
  end
end
