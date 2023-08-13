package jdbc.network;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import plan.Planner;
import server.SimpleDB;
import tx.Transaction;

public class RemoteConnectionImpl extends UnicastRemoteObject implements RemoteConnection {
  private SimpleDB db;
  private Transaction currentTx;
  private Planner planner;

  protected RemoteConnectionImpl(SimpleDB db) throws RemoteException {
    this.db = db;
    currentTx = db.newTx();
    planner = db.planner();
  }

  @Override
  public RemoteStatement createStatement() throws RemoteException {
    return new RemoteStatementImpl(this, planner);
  }

  @Override
  public void close() throws RemoteException {
    currentTx.commit();
  }

  Transaction getTransaction() {
    return currentTx;
  }

  void commit() {
    currentTx.commit();
    currentTx = db.newTx();
  }

  void rollback() {
    currentTx.rollback();
    currentTx = db.newTx();
  }
}
