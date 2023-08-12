package jdbc.network;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface RemoteDriver extends Remote {
  public RemoteConnection connect() throws RemoteException;
}
