defprotocol UDB.Store.Atomic do
  def init(atomic, opts)
  def rollback(atomic)
  def commit(atomic, opts \\ [])
end
