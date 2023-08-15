package jdbc.network;

import java.sql.SQLException;
import java.sql.Statement;
import jdbc.ConnectionAdapter;

public class NetworkConnection extends ConnectionAdapter {
  private RemoteConnection rconn;

  public NetworkConnection(RemoteConnection rconn) {
    this.rconn = rconn;
  }

  @Override
  public Statement createStatement() throws SQLException {
    try {
      var rstmt = rconn.createStatement();
      return new NetworkStatement(rstmt);
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  @Override
  public void close() throws SQLException {
    try {
      rconn.close();
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }
}
