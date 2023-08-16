package server;

import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import jdbc.network.RemoteDriver;
import jdbc.network.RemoteDriverImpl;

public class StartServer {
  public static void main(String[] args) throws RemoteException, AlreadyBoundException {
    var dirname = (args.length == 0) ? "studentdb" : args[0];
    var db = new SimpleDB(dirname);

    var reg = LocateRegistry.createRegistry(1099);

    RemoteDriver d = new RemoteDriverImpl(db);
    reg.bind("simpledb", d);

    System.out.println("database server ready");
  }
}
