package jdbc.embedded;

import static java.sql.Types.INTEGER;

import java.sql.SQLException;
import jdbc.ResultSetMetaDataAdapter;
import record.Schema;

public class EmbeddedResultSetMetaData extends ResultSetMetaDataAdapter {
  private Schema sch;

  public EmbeddedResultSetMetaData(Schema sch) {
    this.sch = sch;
  }

  @Override
  public int getColumnCount() throws SQLException {
    return sch.fields().size();
  }

  @Override
  public String getColumnName(int column) throws SQLException {
    return sch.fields().get(column - 1);
  }

  @Override
  public int getColumnType(int column) throws SQLException {
    var fldname = getColumnName(column);
    return sch.type(fldname);
  }

  public int getColumnDisplaySize(int column) throws SQLException {
    var fldname = getColumnName(column);
    var fldtype = sch.type(fldname);
    var fldlength = (fldtype == INTEGER) ? 6 : sch.length(fldname);
    return Math.max(fldname.length(), fldlength) + 1;
  }
}
