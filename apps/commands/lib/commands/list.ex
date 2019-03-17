defmodule UDB.Commands.List do
  use Bitwise

  @list "list"
  @meta "list-meta"
  @separator ":"
  @list_min_seq 1000
  @list_max_seq (1 <<< 31) - 1000
  @list_initial_seq round(@list_min_seq + (@list_max_seq - @list_min_seq) / 2)
  @store UDB.Store

  def all(conn, key),
    do: atomic(conn, fn atomic -> unsafe_all(atomic, key) end)

  defp unsafe_all(conn, key) do
    conn
    |> @store.stream({:fwd_prefix, encode_prefix_key(key)})
    |> Stream.map(fn {_key, value} -> value end)
    |> Enum.into([])
  end

  def index(conn, key, index),
    do: atomic(conn, fn atomic -> unsafe_index(atomic, key, index) end)

  # TODO: Check if we should use `RocksDBStore.Iterator.move/2` instead of `RocksDBStore.get/2`
  defp unsafe_index(conn, key, index) do
    meta_key = encode_meta_key(key)
    with {:ok, head, tail, _size} <- get_meta(conn, meta_key) do
      item_key = index_to_item_key(key, {head, tail}, index)
      @store.get(conn, item_key)
    end
  end

  def range(conn, key, start, stop),
    do: atomic(conn, fn atomic -> unsafe_range(atomic, key, start, stop) end)

  defp unsafe_range(conn, key, start, stop) do
    meta_key = encode_meta_key(key)
    with {:ok, head, tail, _size} <- get_meta(conn, meta_key) do
      start_key = index_to_item_key(key, {head, tail}, start)
      stop_key = index_to_item_key(key, {head, tail}, stop)

      conn
      |> @store.stream({:fwd_incl, {start_key, stop_key}})
      |> Stream.map(fn {_key, value} -> value end)
      |> Enum.into([])
    end
  end

  def set(conn, key, index, value),
    do: atomic(conn, fn atomic -> unsafe_set(atomic, key, index, value) end)

  defp unsafe_set(conn, key, index, value) do
    meta_key = encode_meta_key(key)
    with {:ok, head, tail, size} <- get_meta(conn, meta_key) do
      sequence = index_to_sequence({head, tail}, index)

      cond do
        size == 0 ->
          {:error, {:index, :out_of_range}}

        sequence >= head && sequence <= tail ->
          item_key = encode_key(key, sequence)
          @store.put(conn, item_key, value)

        true ->
          {:error, {:index, :out_of_range}}
      end
    end
  end

  def trim(conn, key, start, stop),
    do: atomic(conn, fn atomic -> unsafe_trim(atomic, key, start, stop) end)

  defp unsafe_trim(conn, key, start, stop) do
    meta_key = encode_meta_key(key)
    with {:ok, head, _, size} <- get_meta(conn, meta_key) do
      start = normalize_index(start, size)
      stop = normalize_index(stop, size)

      # Delete all list if given range returns empty sublist
      if start >= size || start > stop do
        # TODO: Rewrite to using `@store.delete_range`
        conn
        |> @store.stream({:fwd_prefix, encode_prefix_key(key)})
        |> Enum.each(fn {item_key, _} -> @store.delete(conn, item_key) end)
      end

      start = if start < 0, do: 0, else: start
      stop = if stop >= size, do: size - 1, else: stop

      if start > 0 do
        # TODO: Rewrite to using `@store.delete_range`
        Enum.each(0..(start - 1), fn idx ->
          item_key = encode_key(key, head + idx)
          @store.delete(conn, item_key)
        end)
      end

      if stop < size - 1 do
        # TODO: Rewrite to using `@store.delete_range`
        Enum.each((stop + 1)..(size - 1), fn idx ->
          item_key = encode_key(key, head + idx)
          @store.delete(conn, item_key)
        end)
      end

      set_meta(conn, meta_key, {head + start, head + stop})
    end
  end

  def rpush(conn, key, value) when is_binary(value),
    do: atomic(conn, fn atomic -> unsafe_push(atomic, key, [value], :tail) end)
  def rpush(conn, key, values) when is_list(values),
    do: atomic(conn, fn atomic -> unsafe_push(atomic, key, values, :tail) end)

  def rpop(conn, key),
    do: atomic(conn, fn atomic -> unsafe_pop(atomic, key, :tail) end)

  def lpush(conn, key, value) when is_binary(value),
    do: atomic(conn, fn atomic -> unsafe_push(atomic, key, [value], :head) end)
  def lpush(conn, key, values) when is_list(values),
    do: atomic(conn, fn atomic -> unsafe_push(atomic, key, values, :head) end)

  def lpop(conn, key),
    do: atomic(conn, fn atomic -> unsafe_pop(atomic, key, :head) end)

  def len(conn, key),
    do: atomic(conn, fn atomic -> unsafe_len(atomic, key) end)

  defp unsafe_len(conn, key) do
    with meta_key = encode_meta_key(key), {:ok, _, _, size} <- get_meta(conn, meta_key),
      do: size
  end

  defp unsafe_push(conn, key, values, sequence_type) do
    meta_key = encode_meta_key(key)
    with {:ok, head, tail, size} <- get_meta(conn, meta_key) do
      {sequence, delta} = case {sequence_type, size} do
        {:head, 0} -> {head, -1}
        {:tail, 0} -> {tail, 1}
        {:head, _} -> {head - 1, -1}
        {:tail, _} -> {tail + 1, 1}
      end

      values
      |> Enum.with_index()
      |> Enum.each(fn {value, index} ->
        item_key = encode_key(key, sequence + index * delta)
        @store.put(conn, item_key, value)
      end)

      length = length(values)
      new_meta = case sequence_type do
        :head -> {sequence + ((length - 1) * delta), tail}
        :tail -> {head, sequence + ((length - 1) * delta)}
      end

      set_meta(conn, meta_key, new_meta)
      size + length
    end
  end

  defp unsafe_pop(conn, key, sequence_type) do
    meta_key = encode_meta_key(key)
    with {:ok, head, tail, _size} <- get_meta(conn, meta_key) do
      {sequence, next_meta} = case sequence_type do
        :head -> {head, {head + 1, tail}}
        :tail -> {tail, {head, tail - 1}}
      end

      item_key = encode_key(key, sequence)
      value = @store.get(conn, item_key)
      @store.delete(conn, item_key)

      set_meta(conn, meta_key, next_meta)
      value
    end
  end

  defp get_meta(conn, meta_key) do
    case  @store.get(conn, meta_key) do
      <<head::integer-size(32), tail::integer-size(32)>> -> {:ok, head, tail, tail - head + 1}
      nil -> {:ok, @list_initial_seq, @list_initial_seq, 0}
      _ -> {:error, {:meta, :invalid_format}}
    end
  end

  defp set_meta(conn, meta_key, {head, tail}) do
    with :ok <- check_meta({head, tail}) do
      @store.put(conn, meta_key, <<head::integer-size(32), tail::integer-size(32)>>)
    end
  end

  defp check_meta({head, tail}) do
    case check_sequence(head) && check_sequence(tail) && (head + tail) >= 0 do
      true -> :ok
      false -> {:error, {:meta, :invalid}}
    end
  end

  defp check_sequence(sequence),
    do: sequence > @list_min_seq && sequence < @list_max_seq

  defp index_to_item_key(key, meta, index),
    do: encode_key(key, index_to_sequence(meta, index))

  defp index_to_sequence({head, _}, index) when index >= 0,
    do: head + index
  defp index_to_sequence({_, tail}, index) when index < 0,
    do: tail + index + 1

  defp normalize_index(index, size) when index < 0,
    do: size + index
  defp normalize_index(index, _size),
    do: index

  defp atomic(conn, fun, opts \\ []) do
    if @store.support_atomic?(conn),
      do: @store.atomic(conn, fun, opts),
      else: fun.(conn)
  end

  defp encode_meta_key(key),
    do: "#{@meta}#{@separator}#{key}"

  defp encode_key(key, sequence),
    do: "#{@list}#{@separator}#{key}#{@separator}" <> <<sequence::integer-size(32)>>

  defp encode_prefix_key(key),
    do: "#{@list}#{@separator}#{key}#{@separator}"
end
