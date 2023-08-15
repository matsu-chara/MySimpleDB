package jdbc.network;

import java.sql.SQLException;
import jdbc.ResultSetMetaDataAdapter;

public class NetworkMetaData extends ResultSetMetaDataAdapter {
  private RemoteMetaData rmd;

  public NetworkMetaData(RemoteMetaData rmd) {
    this.rmd = rmd;
  }

  @Override
  public int getColumnCount() throws SQLException {
    try {
      return rmd.getColumnCount();
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  @Override
  public String getColumnName(int column) throws SQLException {
    try {
      return rmd.getColumnName(column);
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  @Override
  public int getColumnType(int column) throws SQLException {
    try {
      return rmd.getColumnType(column);
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }

  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    try {
      return rmd.getColumnDisplaySize(column);
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }
}
