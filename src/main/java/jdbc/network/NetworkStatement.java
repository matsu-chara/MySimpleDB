package jdbc.network;

import java.sql.ResultSet;
import java.sql.SQLException;
import jdbc.StatementAdapter;

public class NetworkStatement extends StatementAdapter {
  private RemoteStatement rstmt;

  public NetworkStatement(RemoteStatement rstmt) {
    this.rstmt = rstmt;
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    try {
      return new NetworkResultSet(rstmt.executeQuery(sql));
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    try {
      return rstmt.executeUpdate(sql);
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  @Override
  public void close() throws SQLException {
    try {
      rstmt.close();
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }
}
