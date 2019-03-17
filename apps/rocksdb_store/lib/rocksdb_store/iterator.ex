defmodule UDB.RocksDBStore.Iterator do
  @moduledoc """
  An iterator can be used to traverse the data store.
  """

  defstruct [ref: nil, state: :closed, keys_only: false, opts: []]

  @doc """
  Opens and returns an interator which can be used to traverse the
  data store using the `move/2` function. It can be
  closed again using the `close/1` function.
  """
  def create(%UDB.RocksDBStore{ref: dbref, state: :open}, opts \\ []) do
    keys_only = Keyword.get(opts, :keys_only, false)
    opts = Keyword.delete(opts, :keys_only)

    {:ok, iterator_ref} = :rocksdb.iterator(dbref, opts)
    {:ok, %__MODULE__{ref: iterator_ref, state: :open, keys_only: keys_only, opts: opts}}
  end

  @doc """
  Move the pointer in a data store using the following actions:

    * :first - move to the first element
    * :next - move forward
    * :prev - move backwards
    * :last - move to last
    * key - move to the closest key
    * {:seek, key} - move to the closest key
    * {:seek_for_prev, key} - move to the closest previous key

  It is also posible to give it a binary in which case it will move
  the pointer to the closest key.

  Every operation will return `{:ok, key, value}`, or `{:ok, key}` if the `:keys_only` option was set.
  """
  def move(%__MODULE__{ref: iterator_ref, state: :open, keys_only: keys_only}, action) do
    case :rocksdb.iterator_move(iterator_ref, action) do
      {:ok, key, value} -> if keys_only, do: {:ok, key}, else: {:ok, key, value}
      error             -> error
    end
  end

  @doc """
  Close the iterator
  """
  def close(%__MODULE__{state: :closed} = iterator), do: iterator
  def close(%__MODULE__{ref: iterator_ref, state: :open}) do
    :ok = :rocksdb.iterator_close(iterator_ref)
    %__MODULE__{ref: nil, state: :closed}
  end
end
