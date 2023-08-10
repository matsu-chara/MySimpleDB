package jdbc.embedded;

import java.sql.SQLException;
import java.util.Properties;
import jdbc.DriverAdapter;
import server.SimpleDB;

public class EmbeddedDriver extends DriverAdapter {
  @Override
  public EmbeddedConnection connect(String url, Properties p) throws SQLException {
    String dbname = url.replace("jdbc:simpledb:", "");
    SimpleDB db = new SimpleDB(dbname);
    return new EmbeddedConnection(db);
  }
}
