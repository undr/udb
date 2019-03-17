defprotocol UDB.Store.Access do
  def get(conn, key, opts \\ [])
  def put(conn, key, value, opts \\ [])
  def delete(conn, key, opts \\ [])
  def stream(conn, query, opts \\ [])
end
