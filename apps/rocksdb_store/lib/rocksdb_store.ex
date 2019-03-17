defmodule UDB.RocksDBStore do
  defstruct name: nil, ref: nil, state: nil

  @doc """
  Opens a connection to a data store
  """
  def open(name, opts \\ []) when is_binary(name) do
    case :rocksdb.open(String.to_charlist(name), opts) do
      {:ok, ref} -> {:ok, %__MODULE__{name: name, ref: ref, state: :open}}
      error      -> error
    end
  end

  @doc """
  Create a new RocksDB database and open a connection
  """
  def create(name, opts \\ []) when is_binary(name),
    do: open(name, Keyword.merge(opts, [create_if_missing: true, error_if_exists: true]))

  def destroy(name, opts \\ []) when is_binary(name),
    do: :rocksdb.destroy(String.to_charlist(name), opts)
end

defimpl UDB.Store.AtomicBuilder, for: UDB.RocksDBStore do
  alias UDB.RocksDBStore

  def supported?(_conn),
    do: true

  def create(%{state: :open} = conn),
    do: RocksDBStore.Atomic.create(conn)
end


defimpl UDB.Store.Maintain, for: UDB.RocksDBStore do
  alias UDB.RocksDBStore

  @doc """
  Close the connection to a data store
  """
  def close(%{state: :closed}),
    do: {:error, :already_closed}
  def close(%{state: :destroyed}),
    do: {:error, :already_destroyed}
  def close(%{ref: dbref, state: :open} = conn) do
    try do
      with :ok <- :rocksdb.close(dbref) do
        {:ok, %{conn | ref: nil, state: :closed}}
      end
    rescue
      _ in ArgumentError -> {:error, :already_closed}
    end
  end

  @doc """
  Destroy and delete a data store. This can not be undone.
  Will close the connection if it is open.
  """
  def destroy(conn, opts \\ [])
  def destroy(%{state: :destroyed}, _opts),
    do: {:error, :already_destroyed}
  def destroy(%{state: :open} = conn, opts) do
    with {:ok, closed_conn} <- close(conn) do
      destroy(closed_conn, opts)
    else
      _ -> {:error, :already_destroyed}
    end
  end
  def destroy(%{name: name, state: :closed} = conn, opts) do
    try do
      :ok = RocksDBStore.destroy(name, opts)
      {:ok, %{conn | state: :destroyed}}
    rescue
      _ in ArgumentError -> {:error, :already_destroyed}
    end
  end

  @doc """
  Repair a data store. Will close the connection if the input is a connection.
  """
  def repair(conn, opts \\ [])
  def repair(%{state: :open} = conn, opts),
    do: with {:ok, closed_conn} <- close(conn), do: repair(closed_conn, opts)
  def repair(%{name: name, state: :closed} = conn, opts) do
    :ok = :rocksdb.repair(String.to_charlist(name), opts)
    {:ok, conn}
  end

  def is_empty?(%{ref: dbref, state: :open}),
    do: :rocksdb.is_empty(dbref)
end

defimpl UDB.Store.Access, for: UDB.RocksDBStore do
  def get(%{state: :open} = conn, key),
    do: get(conn, key, nil, [])
  def get(%{state: :open} = conn, key, default) when is_binary(default),
    do: get(conn, key, default, [])
  def get(%{state: :open} = conn, key, opts) when is_list(opts),
    do: get(conn, key, nil, opts)
  def get(%{ref: dbref, state: :open}, key, default \\ nil, opts \\ []) do
    case :rocksdb.get(dbref, key, opts) do
      {:ok, value} -> value
      :not_found   -> default
      error        -> error
    end
  end

  def put(%{ref: dbref, state: :open}, key, value, opts \\ []),
    do: :rocksdb.put(dbref, key, value, opts)

  def delete(%{ref: dbref, state: :open}, key, opts \\ []) when is_binary(key),
    do: :rocksdb.delete(dbref, key, opts)

  def stream(%{state: :open} = conn, query, opts \\ []),
    do: UDB.RocksDBStore.Stream.create(conn, query, opts)
end

defimpl Collectable, for: UDB.RocksDBStore do
  def into(%{state: :open} = conn) do
    {conn, fn
      _, {:cont, {key, value}} -> UDB.Store.put(conn, key, value)
      _, {:cont, key} when is_binary(key) -> UDB.Store.put(conn, key, key)
      _, :done -> conn
      _, :halt -> :ok
    end}
  end
end
