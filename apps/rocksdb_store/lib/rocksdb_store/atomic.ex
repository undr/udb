defmodule UDB.RocksDBStore.Atomic do
  defstruct [
    conn: nil,
    batch: nil,
    snapshot: nil,
    read_opts: [],
    write_opts: [],
    state: nil
  ]

  def create(conn) do
    {:ok, %__MODULE__{conn: conn}}
  end

  defimpl UDB.Store.Access do
    def get(atomic, key),
      do: get(atomic, key, nil, [])
    def get(atomic, key, default) when is_binary(default),
      do: get(atomic, key, default, [])
    def get(atomic, key, opts) when is_list(opts),
      do: get(atomic, key, nil, opts)
    def get(%{conn: %{ref: dbref}, snapshot: snapshot, read_opts: read_opts}, key, default, opts) do
      opts = Keyword.merge(read_opts, Keyword.merge(opts, snapshot: snapshot))
      case :rocksdb.get(dbref, key, opts) do
        {:ok, value} -> value
        :not_found   -> default
        error        -> error
      end
    end

    def stream(%{conn: conn, snapshot: snapshot, read_opts: read_opts}, query, opts \\ []) do
      opts = Keyword.merge(read_opts, Keyword.merge(opts, snapshot: snapshot))
      UDB.RocksDBStore.Stream.create(conn, query, opts)
    end

    def put(%{batch: batch}, key, value, _opts \\ []),
      do: :rocksdb.batch_put(batch, key, value)

    def delete(%{batch: batch}, key, _opts \\ []),
      do: :rocksdb.batch_delete(batch, key)
  end

  defimpl UDB.Store.Atomic, for: UDB.RocksDBStore.Atomic do
    def init(%{conn: %{ref: dbref}} = atomic, opts \\ []) do
      with {:ok, snapshot} <- :rocksdb.snapshot(dbref),
           {:ok, batch} <- :rocksdb.batch() do
        read_opts = Keyword.get(opts, :read_opts, [])
        write_opts = Keyword.get(opts, :write_opts, [])
        {:ok, %{atomic | batch: batch, snapshot: snapshot, read_opts: read_opts, write_opts: write_opts, state: :open}}
      end
    end

    def commit(%{conn: conn, batch: batch, snapshot: snapshot, write_opts: write_opts}, opts \\ []) do
      with :ok <- :rocksdb.write_batch(conn.ref, batch, Keyword.merge(write_opts, opts)),
           :ok <- :rocksdb.release_batch(batch),
           :ok <- :rocksdb.release_snapshot(snapshot), do: :ok
    end

    def rollback(%{batch: batch, snapshot: snapshot}) do
      with :ok <- :rocksdb.release_batch(batch),
           :ok <- :rocksdb.release_snapshot(snapshot), do: :ok
    end
  end
end
