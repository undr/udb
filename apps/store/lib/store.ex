defmodule UDB.Store do
  @moduledoc """
  Documentation for `UDB.Store`.
  """

  alias UDB.Store.Atomic
  alias UDB.Store.Access
  alias UDB.Store.Maintain
  alias UDB.Store.AtomicBuilder

  def open(store, name, opts \\ []),
    do: store.open(name, opts)

  def create(store, name, opts \\ []),
    do: store.create(name, opts)

  def close(conn),
    do: Maintain.close(conn)

  def destroy(conn, opts \\ []),
    do: Maintain.destroy(conn, opts)

  def repair(%{state: :open} = conn, opts \\ []),
    do: Maintain.repair(conn, opts)

  def is_empty?(%{state: :open} = conn),
    do: Maintain.is_empty?(conn)

  def support_atomic?(conn),
    do: AtomicBuilder.supported?(conn)

  def atomic(%{state: :open} = conn, func, opts \\ []) do
    with {:ok, atomic} <- AtomicBuilder.create(conn),
         {:ok, atomic} <- Atomic.init(atomic, opts) do
      try do
        func.(atomic)
      rescue
        exception ->
          Atomic.rollback(atomic)
          raise exception
      catch
        :exit, code ->
          Atomic.rollback(atomic)
          exit(code)
        any ->
          Atomic.rollback(atomic)
          throw(any)
      else
        value ->
          Atomic.commit(atomic)
          value
      end
    end
  end

  def get(%{state: :open} = conn, key, opts \\ []),
    do: Access.get(conn, key, opts)

  def put(%{state: :open} = conn, key, value, opts \\ []),
    do: Access.put(conn, key, value, opts)

  def delete(%{state: :open} = conn, key, opts \\ []),
    do: Access.delete(conn, key, opts)

  def stream(%{state: :open} = conn, query, opts \\ []),
    do: Access.stream(conn, query, opts)
end
