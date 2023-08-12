package jdbc.network;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface RemoteStatement extends Remote {
  public RemoteResultSet executeQuery(String qry) throws RemoteException;

  public int executeUpdate(String cmd) throws RemoteException;

  public void close() throws RemoteException;
}
