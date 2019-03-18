defmodule UDB.ETSStore.Stream do
	@moduledoc """
  Implements various stream operations
  """

  def create(connection, query, opts \\ []) do
    case parse_query(query) do
      {_, nil} -> []
      {direction, expression} -> stream(connection, direction, expression, opts)
      rest -> IO.inspect(rest)
    end
  end

  defp stream(connection, direction, expression, opts) do
    values = make_request(connection, expression, direction)

    case Keyword.get(opts, :keys_only, false) do
      true -> Stream.map(values, fn {key, _} -> key end)
      false -> Stream.map(values, fn value -> value end)
    end
  end

  defp make_request(conn, expression, :forward),
    do: :ets.select(conn.ref, expression)
  defp make_request(conn, expression, :backward),
    do: :ets.select_reverse(conn.ref, expression)

  defp build_match_expression(first, _) when is_binary(first),
    # do: :ets.fun2ms(fn {key, value} when key >= first -> {key, value} end)
    do: [{{:"$1", :"$2"}, [{:>=, :"$1", first}], [{{:"$1", :"$2"}}]}]
  defp build_match_expression({:first, last}, false) when is_binary(last),
    # do: :ets.fun2ms(fn {key, value} when key < last -> {key, value} end)
    do: [{{:"$1", :"$2"}, [{:<, :"$1", last}], [{{:"$1", :"$2"}}]}]
  defp build_match_expression({:first, last}, true) when is_binary(last),
    # do: :ets.fun2ms(fn {key, value} when key <= last -> {key, value} end)
    do: [{{:"$1", :"$2"}, [{:"=<", :"$1", last}], [{{:"$1", :"$2"}}]}]
  defp build_match_expression({first, last}, false) when is_binary(first) and is_binary(last),
    # do: :ets.fun2ms(fn {key, value} when key >= first and key < last -> {key, value} end)
    do: [{{:"$1", :"$2"}, [{:andalso, {:>=, :"$1", first}, {:<, :"$1", last}}], [{{:"$1", :"$2"}}]}]
  defp build_match_expression({first, last}, true) when is_binary(first) and is_binary(last),
    # do: :ets.fun2ms(fn {key, value} when key >= first and key <= last -> {key, value} end)
    do: [{{:"$1", :"$2"}, [{:andalso, {:>=, :"$1", first}, {:"=<", :"$1", last}}], [{{:"$1", :"$2"}}]}]
  defp build_match_expression(_, _),
    do: nil
  defp build_match_expression({:prefix, prefix}),
    do: [{{:"$1", :"$2"}, [{:andalso, {:>=, :"$1", prefix}, {:"=<", :"$1", "#{prefix}\xff"}}], [{{:"$1", :"$2"}}]}]
  defp build_match_expression(_),
    do: nil

  defp parse_query({direction, range}) when direction in [:fwd, :fwd_incl, :fwd_excl],
    do: {:forward, build_match_expression(range, direction in [:fwd, :fwd_incl])}
  defp parse_query({:fwd_prefix, prefix}) when is_binary(prefix),
    do: {:forward, build_match_expression({:prefix, prefix})}
  defp parse_query({direction, range}) when direction in [:bwd, :bwd_incl, :bwd_excl],
    do: {:backward, build_match_expression(range, direction in [:bwd, :bwd_incl])}
  defp parse_query({:bwd_prefix, prefix}) when is_binary(prefix),
    do: {:backward, build_match_expression({:prefix, prefix})}
end
