defprotocol UDB.Store.AtomicBuilder do
  def supported?(conn)
  def create(conn)
end
