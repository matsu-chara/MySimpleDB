package jdbc.network;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import server.SimpleDB;

public class RemoteDriverImpl extends UnicastRemoteObject implements RemoteDriver {
  private SimpleDB db;

  public RemoteDriverImpl(SimpleDB db) throws RemoteException {
    this.db = db;
  }

  @Override
  public RemoteConnection connect() throws RemoteException {
    return new RemoteConnectionImpl(db);
  }
}
