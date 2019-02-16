defmodule UDB.RocksDBStore.Atomic do
  alias UDB.RocksDBStore

  defstruct [
    conn: nil,
    batch: nil,
    snapshot: nil,
    read_opts: [],
    write_opts: [],
    state: :closed
  ]

  def execute(%RocksDBStore{status: :open} = connection, func, opts \\ []) do
    case create(connection, opts) do
      {:ok, atomic} ->
        try do
          func.(atomic)
        rescue
          exception ->
            rollback(atomic)
            raise exception
        catch
          :exit, code ->
            rollback(atomic)
            exit(code)
          any ->
            rollback(atomic)
            throw(any)
        after
          commit(atomic)
        end

      error -> error
    end
  end

  def get(%__MODULE__{state: :open} = atomic, key),
    do: get(atomic, key, nil, [])
  def get(%__MODULE__{state: :open} = atomic, key, default) when is_binary(default),
    do: get(atomic, key, default, [])
  def get(%__MODULE__{state: :open} = atomic, key, opts) when is_list(opts),
    do: get(atomic, key, nil, opts)
  def get(%__MODULE__{conn: conn, snapshot: snapshot, read_opts: read_opts, state: :open}, key, default, opts) do
    opts = Keyword.merge(read_opts, Keyword.merge(opts, snapshot: snapshot))
    case :rocksdb.get(conn.ref, key, opts) do
      {:ok, value} -> value
      :not_found   -> default
      error        -> error
    end
  end

  def stream(%__MODULE__{conn: conn, snapshot: snapshot, read_opts: read_opts, state: :open}, query, opts \\ []) do
    opts = Keyword.merge(read_opts, Keyword.merge(opts, snapshot: snapshot))
    RocksDBStore.stream(conn, query, opts)
  end

  def put(%__MODULE__{batch: batch, state: :open}, key, value),
    do: :rocksdb.batch_put(batch, key, value)

  def delete(%__MODULE__{batch: batch, state: :open}, key),
    do: :rocksdb.batch_delete(batch, key)

  def commit(%__MODULE__{conn: conn, state: :open} = atomic, opts \\ []) do
    with :ok <- :rocksdb.write_batch(conn.ref, atomic.batch, Keyword.merge(atomic.write_opts, opts)),
         :ok <- :rocksdb.release_batch(atomic.batch),
         :ok <- :rocksdb.release_snapshot(atomic.snapshot), do: :ok
  end

  def rollback(%__MODULE__{batch: batch, snapshot: snapshot, state: :open}) do
    with :ok <- :rocksdb.release_batch(batch),
         :ok <- :rocksdb.release_snapshot(snapshot), do: :ok
  end

  defp create(%RocksDBStore{ref: dbref, status: :open} = connection, opts) do
    {:ok, snapshot} = :rocksdb.snapshot(dbref)
    {:ok, batch} = :rocksdb.batch()
    read_opts = Keyword.get(opts, :read_opts, [])
    write_opts = Keyword.get(opts, :write_opts, [])

    {:ok, %__MODULE__{
      conn: connection,
      batch: batch,
      snapshot: snapshot,
      read_opts: read_opts,
      write_opts: write_opts,
      state: :open
    }}
  end
end
