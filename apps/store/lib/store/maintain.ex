defprotocol UDB.Store.Maintain do
  def close(conn)
  def destroy(conn, opts \\ [])
  def repair(conn, opts \\ [])
  def is_empty?(conn)
end
