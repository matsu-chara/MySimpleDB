package jdbc.network;

import java.rmi.registry.LocateRegistry;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import jdbc.DriverAdapter;

public class NetworkDriver extends DriverAdapter {
  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    try {
      var host = url.replace("jdbc:simpledb://", "");
      var reg = LocateRegistry.getRegistry(host, 1099);
      var rdvr = (RemoteDriver) reg.lookup("simpledb");
      var rconn = rdvr.connect();
      return new NetworkConnection(rconn);
    } catch (Exception e) {
      throw new SQLException(e);
    }
  }
}
