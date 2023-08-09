package jdbc.embedded;

import java.sql.SQLException;
import java.util.Locale;
import jdbc.ResultSetAdapter;
import plan.Plan;
import query.Scan;
import record.Schema;

public class EmbeddedResultSet extends ResultSetAdapter {
  private Scan s;
  private Schema sch;
  private EmbeddedConnection conn;

  public EmbeddedResultSet(Plan plan, EmbeddedConnection conn) {
    s = plan.open();
    sch = plan.schema();
    this.conn = conn;
  }

  @Override
  public boolean next() throws SQLException {
    try {
      return s.next();
    } catch (RuntimeException e) {
      conn.rollback();
      throw new SQLException(e);
    }
  }

  @Override
  public int getInt(String fldname) throws SQLException {
    try {
      fldname = fldname.toLowerCase(Locale.US);
      return s.getInt(fldname);
    } catch (RuntimeException e) {
      conn.rollback();
      throw new SQLException(e);
    }
  }

  @Override
  public String getString(String fldname) throws SQLException {
    try {
      fldname = fldname.toLowerCase(Locale.US);
      return s.getString(fldname);
    } catch (RuntimeException e) {
      conn.rollback();
      throw new SQLException(e);
    }
  }

  @Override
  public EmbeddedResultSetMetaData getMetaData() throws SQLException {
    return new EmbeddedResultSetMetaData(sch);
  }

  @Override
  public void close() throws SQLException {
    s.close();
    conn.commit();
  }
}
