defmodule UDB.Store.Access.StreamTestCase do
  defmacro __using__(opts) do
    {:ok, module} = Keyword.fetch(opts, :module)

    quote location: :keep do
      alias UDB.Store

      describe "#{unquote(module)}.stream" do
        use UDB.Store.TestCase, unquote(opts)

        def stream(conn, query, opts \\ []) do
          Store.stream(conn, query, opts) |> Enum.into([])
        end

        def seed_data(conn, %{sparse: true}) do
          ["a2", "a4", "b2", "b3", "b5", "c1", "c3", "c4", "d2", "d4"] |> Enum.into(conn)
        end

        @tag sparse: true
        test "fwd range stream for sparse list", %{conn: conn} do
          # to last
          assert stream(conn, {:fwd, "c3"}, keys_only: true) == ["c3", "c4", "d2", "d4"]
          # from first excluding last
          assert stream(conn, {:fwd_excl, {:first, "c2"}}, keys_only: true) == ["a2", "a4", "b2", "b3", "b5", "c1"]
          # from first including last
          assert stream(conn, {:fwd_incl, {:first, "c2"}}, keys_only: true) == ["a2", "a4", "b2", "b3", "b5", "c1"]

          # excluding last
          assert stream(conn, {:fwd_excl, {"b2", "c2"}}, keys_only: true) == ["b2", "b3", "b5", "c1"]
          # including last
          assert stream(conn, {:fwd_incl, {"b2", "c2"}}, keys_only: true) == ["b2", "b3", "b5", "c1"]
        end

        @tag sparse: true
        test "fwd prefix stream for sparse list", %{conn: conn} do
          assert stream(conn, {:fwd_prefix, "b"}, keys_only: true) == ["b2", "b3", "b5"]
        end

        @tag sparse: true
        test "bwd range stream for sparse list", %{conn: conn} do
          # from last
          assert stream(conn, {:bwd, "c2"}, keys_only: true) == ["d4", "d2", "c4", "c3"]
          # to first including last
          assert stream(conn, {:bwd_incl, {:first, "c2"}}, keys_only: true) == ["c1", "b5", "b3", "b2", "a4", "a2"]
          # to first excluding last
          assert stream(conn, {:bwd_excl, {:first, "c2"}}, keys_only: true) == ["c1", "b5", "b3", "b2", "a4", "a2"]
          # including last
          assert stream(conn, {:bwd_incl, {"b2", "c2"}}, keys_only: true) == ["c1", "b5", "b3", "b2"]
          # excluding last
          assert stream(conn, {:bwd_excl, {"b2", "c2"}}, keys_only: true) == ["c1", "b5", "b3", "b2"]
        end

        @tag sparse: true
        test "bwd prefix stream for sparse list", %{conn: conn} do
          assert stream(conn, {:bwd_prefix, "b"}, keys_only: true) == ["b5", "b3", "b2"]
        end

        def seed_data(conn, %{}) do
          ["a1", "a2", "b1", "b2", "b3", "c1", "c2", "c3", "d1", "d2"] |> Enum.into(conn)
        end

        test "fwd range stream", %{conn: conn} do
          # to last
          assert stream(conn, {:fwd, "c2"}, keys_only: true) == ["c2", "c3", "d1", "d2"]
          # from first excluding last
          assert stream(conn, {:fwd_excl, {:first, "c2"}}, keys_only: true) == ["a1", "a2", "b1", "b2", "b3", "c1"]
          # from first including last
          assert stream(conn, {:fwd_incl, {:first, "c2"}}, keys_only: true) == ["a1", "a2", "b1", "b2", "b3", "c1", "c2"]
          # excluding last
          assert stream(conn, {:fwd_excl, {"b2", "c2"}}, keys_only: true) == ["b2", "b3", "c1"]
          # including last
          assert stream(conn, {:fwd_incl, {"b2", "c2"}}, keys_only: true) == ["b2", "b3", "c1", "c2"]
        end

        test "fwd prefix stream", %{conn: conn} do
          assert stream(conn, {:fwd_prefix, "b"}, keys_only: true) == ["b1", "b2", "b3"]
        end

        test "bwd range stream", %{conn: conn} do
          # from last
          assert stream(conn, {:bwd, "c2"}, keys_only: true) == ["d2", "d1", "c3", "c2"]
          # to first including last
          assert stream(conn, {:bwd_incl, {:first, "c2"}}, keys_only: true) == ["c2", "c1", "b3", "b2", "b1", "a2", "a1"]
          # to first excluding last
          assert stream(conn, {:bwd_excl, {:first, "c2"}}, keys_only: true) == ["c1", "b3", "b2", "b1", "a2", "a1"]
          # including last
          assert stream(conn, {:bwd_incl, {"b2", "c2"}}, keys_only: true) == ["c2", "c1", "b3", "b2"]
          # excluding last
          assert stream(conn, {:bwd_excl, {"b2", "c2"}}, keys_only: true) == ["c1", "b3", "b2"]
        end

        test "bwd prefix stream", %{conn: conn} do
          assert stream(conn, {:bwd_prefix, "b"}, keys_only: true) == ["b3", "b2", "b1"]
        end

        test "precise inclusion", %{empty: conn} do
          ["precise", "prefine", "prefinish", "prefix", "prefixable", "prefixal"] |> Enum.into(conn)
          assert stream(conn, {:fwd_incl, {"pref", "prefix"}}, keys_only: true) == ["prefine", "prefinish", "prefix"]
          assert stream(conn, {:fwd_excl, {"pref", "prefix"}}, keys_only: true) == ["prefine", "prefinish"]
          assert stream(conn, {:bwd_incl, {"pref", "prefix"}}, keys_only: true) == ["prefix", "prefinish", "prefine"]
          assert stream(conn, {:bwd_excl, {"pref", "prefix"}}, keys_only: true) == ["prefinish", "prefine"]

          assert stream(conn, {:fwd_incl, {:first, "prefix"}}, keys_only: true) ==
            ["precise", "prefine", "prefinish", "prefix"]
          assert stream(conn, {:fwd_excl, {:first, "prefix"}}, keys_only: true) ==
            ["precise", "prefine", "prefinish"]
          assert stream(conn, {:bwd_incl, {:first, "prefix"}}, keys_only: true) ==
            ["prefix", "prefinish", "prefine", "precise"]
          assert stream(conn, {:bwd_excl, {:first, "prefix"}}, keys_only: true) ==
            ["prefinish", "prefine", "precise"]
        end
      end
    end
  end
end
