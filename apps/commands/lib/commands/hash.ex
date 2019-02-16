defmodule UDB.Commands.Hash do
  alias UDB.RocksDBStore.Atomic

  @hash "hash"
  @size "hash-size"
  @separator ":"
  @store Atomic

  def getall(conn, key),
    do: @store.execute(conn, fn atomic -> unsafe_getall(atomic, key) end)

  def keys(conn, key),
    do: @store.execute(conn, fn atomic -> unsafe_keys(atomic, key) end)

  def values(conn, key),
    do: @store.execute(conn, fn atomic -> unsafe_values(atomic, key) end)

  def get(conn, key, field) do
    @store.execute(conn, fn atomic -> unsafe_get(atomic, key, field) end)
  end

  def delall(conn, key) do
    @store.execute(conn, fn atomic -> unsafe_delall(atomic, key) end)
  end

  def del(conn, key, field) when is_binary(field),
    do: del(conn, key, [field])
  def del(conn, key, fields) do
    @store.execute(conn, fn atomic -> unsafe_del(atomic, key, fields) end)
  end

  def set(conn, key, map) when is_map(map),
    do: set(conn, key, Enum.map(map, fn kv -> kv end))
  def set(conn, key, pair) when is_tuple(pair),
    do: set(conn, key, [pair])
  def set(conn, key, pairs) do
    @store.execute(conn, fn atomic -> unsafe_set(atomic, key, pairs) end)
  end

  def len(conn, key) do
    @store.execute(conn, fn atomic -> unsafe_len(atomic, key) end)
  end

  def unsafe_getall(conn, key),
    do: map(conn, key) |> Enum.into(%{})

  def unsafe_keys(conn, key),
    do: map(conn, key, fn {key, _value} -> key end) |> Enum.into([])

  def unsafe_values(conn, key),
    do: map(conn, key, fn {_key, value} -> value end) |> Enum.into([])

  defp unsafe_get(conn, key, field) do
    item_key = encode_key(key, field)
    @store.get(conn, item_key)
  end

  defp unsafe_delall(conn, key) do
    case unsafe_len(conn, key) do
      0 -> :ok
      _ ->
        conn
        |> @store.stream({:fwd_prefix, encode_prefix_key(key)})
        |> Enum.each(fn {item_key, _} -> @store.delete(conn, item_key) end)
        @store.delete(conn, encode_size_key(key))
    end
  end

  defp unsafe_del(conn, key, fields) do
    incr = Enum.reduce(fields, 0, fn field, acc ->
      item_key = encode_key(key, field)
      case @store.get(conn, item_key) do
        nil ->
          acc
          _ ->
          @store.delete(conn, item_key)
          acc - 1
        end
      end
    )

    incr_size(conn, key, incr)
  end

  defp unsafe_set(conn, key, pairs) do
    incr = Enum.reduce(pairs, 0, fn {field, value}, acc ->
      item_key = encode_key(key, field)
      case @store.get(conn, item_key) do
        nil ->
          @store.put(conn, item_key, value)
          acc + 1
        _ ->
          @store.put(conn, item_key, value)
          acc
      end
    end)

    incr_size(conn, key, incr)
  end

  defp unsafe_len(conn, key) do
    size_key = encode_size_key(key)
    case @store.get(conn, size_key) do
      <<size::integer-size(32)>> -> size
      _ -> 0
    end
  end

  defp map(conn, key, func \\ fn(tuple) -> tuple end) do
    prefix = encode_prefix_key(key)

    try do
      conn
      |> @store.stream({:fwd_prefix, prefix})
      |> Stream.map(fn {item_key, value} -> {extract_field_name(item_key, prefix), value} end)
      |> Stream.map(&(func.(&1)))
    catch
      error -> error
    end
  end

  defp incr_size(_conn, _key, 0), do: :ok
  defp incr_size(conn, key, value) do
    size_key = encode_size_key(key)
    size = case @store.get(conn, size_key) do
      <<size::integer-size(32)>> -> size + value
      _ -> value
    end

    if size <= 0 do
      @store.delete(conn, size_key)
    else
      @store.put(conn, size_key, <<size::integer-size(32)>>)
    end
  end

  defp extract_field_name(item_key, prefix) when is_binary(prefix),
    do: extract_field_name(item_key, byte_size(prefix))
  defp extract_field_name(item_key, size) when is_integer(size) do
    case item_key do
      <<_prefix::binary-size(size), field::binary()>> -> field
      _ -> throw({:error, {:cmd, :cannot_extract_field_name}})
    end
  end

  defp encode_key(key, field),
    do: "#{@hash}#{@separator}#{key}#{@separator}#{field}"

  defp encode_prefix_key(key),
    do: "#{@hash}#{@separator}#{key}#{@separator}"

  defp encode_size_key(key),
    do: "#{@size}#{@separator}#{key}"
end
