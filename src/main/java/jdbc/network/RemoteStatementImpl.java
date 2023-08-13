package jdbc.network;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import plan.Planner;

public class RemoteStatementImpl extends UnicastRemoteObject implements RemoteStatement {
  private RemoteConnectionImpl rconn;
  private Planner planner;

  public RemoteStatementImpl(RemoteConnectionImpl rconn, Planner planner) throws RemoteException {
    this.rconn = rconn;
    this.planner = planner;
  }

  @Override
  public RemoteResultSet executeQuery(String qry) throws RemoteException {
    try {
      var tx = rconn.getTransaction();
      var plan = planner.createQueryPlan(qry, tx);
      return new RemoteResultSetImpl(plan, rconn);
    } catch (RuntimeException e) {
      rconn.rollback();
      throw e;
    }
  }

  @Override
  public int executeUpdate(String cmd) throws RemoteException {
    try {
      var tx = rconn.getTransaction();
      var result = planner.executeUpdate(cmd, tx);
      rconn.commit();
      return result;
    } catch (RuntimeException e) {
      rconn.rollback();
      throw e;
    }
  }

  @Override
  public void close() throws RemoteException {
    // do nothing
  }
}
