package jdbc.network;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import jdbc.ResultSetAdapter;

public class NetworkResultSet extends ResultSetAdapter {
  private RemoteResultSet rrs;

  public NetworkResultSet(RemoteResultSet rrs) {
    this.rrs = rrs;
  }

  @Override
  public boolean next() throws SQLException {
    try {
      return rrs.next();
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  @Override
  public int getInt(String fldname) throws SQLException {
    try {
      return rrs.getInt(fldname);
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  @Override
  public String getString(String fldname) throws SQLException {
    try {
      return rrs.getString(fldname);
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    try {
      var rmd = rrs.getMetaData();
      return new NetworkMetaData(rmd);
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  @Override
  public void close() throws SQLException {
    try {
      rrs.close();
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }
}
