defmodule UDB.RocksDBStore.Stream do
	@moduledoc """
  Implements various stream operations
  """

  alias UDB.RocksDBStore
  alias UDB.RocksDBStore.Iterator

  @type query :: {atom(), binary()} | {atom(), {atom(), binary()}} | {atom(), {binary(), binary()}}

  @spec create(connection :: RocksDBStore.t(), query :: query(), opts :: RocksDBStore.read_options()) :: any()
  def create(connection, query, opts \\ []) do
    {action, start, range_opts} = parse_query(query)
    stream(connection, action, start, Keyword.merge(opts, range_opts))
  end

  defp stream(connection, action, start, opts) do
    Stream.resource(
      fn -> with {:ok, iterator} <- Iterator.create(connection, opts), do: {start, iterator} end,
      fn
        {^start, iterator} -> move(iterator, start)
        iterator           -> move(iterator, action)
      end,
      fn
        {_, iterator} -> move(iterator, start)
        iterator -> Iterator.close(iterator)
      end
    )
  end

  defp move(iterator, action) do
    case Iterator.move(iterator, action) do
      {:ok, key, value} -> {[{key, value}], iterator}
      {:ok, key}        -> {[key], iterator}
      _                 -> {:halt, iterator}
    end
  end

  defp parse_query({:fwd, first}) when is_binary(first),
    do: {:next, first, [iterate_lower_bound: first]}
  defp parse_query({:fwd_excl, {:first, last}}) when is_binary(last),
    do: {:next, :first, [iterate_upper_bound: last]}
  defp parse_query({:fwd_incl, {:first, last}}) when is_binary(last),
    do: {:next, :first, [iterate_upper_bound: "#{last}\x00"]}
  defp parse_query({:fwd_excl, {first, last}}) when is_binary(first) and is_binary(last),
    do: {:next, first, [iterate_lower_bound: first, iterate_upper_bound: last]}
  defp parse_query({:fwd_incl, {first, last}}) when is_binary(first) and is_binary(last),
    do: {:next, first, [iterate_lower_bound: first, iterate_upper_bound: "#{last}\x00"]}

  defp parse_query({:fwd_prefix, prefix}) when is_binary(prefix),
    do: {:next, prefix, [iterate_lower_bound: prefix, iterate_upper_bound: "#{prefix}\xff"]}

  defp parse_query({:bwd, first}) when is_binary(first),
    do: {:prev, :last, [iterate_lower_bound: first]}
  defp parse_query({:bwd_incl, {:first, last}}) when is_binary(last),
    do: {:prev, {:seek_for_prev, last}, []}
  defp parse_query({:bwd_excl, {:first, last}}) when is_binary(last),
    do: {:prev, {:seek_for_prev, last}, [iterate_upper_bound: last]}
  defp parse_query({:bwd_incl, {first, last}}) when is_binary(first) and is_binary(last),
    do: {:prev, {:seek_for_prev, last}, [iterate_lower_bound: first]}
  defp parse_query({:bwd_excl, {first, last}}) when is_binary(first) and is_binary(last),
    do: {:prev, {:seek_for_prev, last}, [iterate_lower_bound: first, iterate_upper_bound: last]}

  defp parse_query({:bwd_prefix, prefix}) when is_binary(prefix),
    do: {:prev, {:seek_for_prev, "#{prefix}\xff"}, [iterate_lower_bound: prefix, iterate_upper_bound: "#{prefix}\xff"]}
end
