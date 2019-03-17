defmodule UDB.ETSStore do
  defstruct [name: nil, ref: nil, state: nil]

  alias UDB.ETSStore.Stream, as: DBStream

  def open(name, opts \\ [])
  def open(name, opts) when is_binary(name),
    do: open(String.to_atom(name), opts)
  def open(name, _opts),
    do: {:ok, %__MODULE__{name: name, ref: :ets.new(name, [:public, :ordered_set]), state: :open}}

  def create(name, opts \\ []),
    do: open(name, opts)

  defimpl UDB.Store.AtomicBuilder do
    def supported?(_conn),
      do: false

    def create(_conn, _opts \\ []),
      do: {:error, :not_implemented}
  end

  defimpl UDB.Store.Maintain do
    def close(%{state: :closed}),
      do: {:error, :already_closed}
    def close(%{state: :destroyed}),
      do: {:error, :already_destroyed}
    def close(%{state: :open, ref: dbref} = conn) do
      try do
        :ets.delete(dbref)
        {:ok, %{conn | ref: nil, state: :closed}}
      rescue
        _ in ArgumentError -> {:error, :already_closed}
      end
    end

    def destroy(conn, opts \\ [])
    def destroy(%{state: :closed}, _opts),
      do: {:error, :already_closed}
    def destroy(%{state: :destroyed}, _opts),
      do: {:error, :already_destroyed}
    def destroy(%{state: :open, ref: dbref} = conn, _opts) do
      try do
        :ets.delete(dbref)
        {:ok, %{conn | ref: nil, state: :destroyed}}
      rescue
        _ in ArgumentError -> {:error, :already_destroyed}
      end
    end

    def repair(conn, _opts \\ []),
      do: {:ok, conn}

    def is_empty?(%{state: :open, ref: dbref}) do
      info = :ets.info(dbref)
      Keyword.get(info, :size, 0) == 0
    end
  end

  defimpl UDB.Store.Access do
    def get(%{ref: dbref}, key, _opts \\ []) do
      case :ets.lookup(dbref, key) do
        [{_, value}] -> value
        [] -> nil
      end
    end

    def put(%{ref: dbref}, key, value, _opts \\ []) do
      :ets.insert(dbref, {key, value})
      :ok
    end

    def delete(%{ref: dbref}, key, _opts \\ []) do
      :ets.delete(dbref, key)
      :ok
    end

    def stream(conn, query, opts \\ []),
      do: DBStream.create(conn, query, opts)
  end

  defimpl Collectable do
    def into(%{state: :open} = conn) do
      {conn, fn
        _, {:cont, {key, value}} -> UDB.Store.put(conn, key, value)
        _, {:cont, key} when is_binary(key) -> UDB.Store.put(conn, key, key)
        _, :done -> conn
        _, :halt -> :ok
      end}
    end
  end
end
