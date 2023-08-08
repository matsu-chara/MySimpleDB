package jdbc.embedded;

import java.sql.SQLException;
import jdbc.StatementAdapter;
import plan.Plan;
import plan.Planner;
import tx.Transaction;

public class EmbeddedStatement extends StatementAdapter {
  private EmbeddedConnection conn;
  private Planner planner;

  public EmbeddedStatement(EmbeddedConnection conn, Planner planner) {
    this.conn = conn;
    this.planner = planner;
  }

  @Override
  public EmbeddedResultSet executeQuery(String qry) throws SQLException {
    try {
      Transaction tx = conn.getTransaction();
      Plan pln = planner.createQueryPlan(qry, tx);
      return new EmbeddedResultSet(pln, conn);
    } catch (RuntimeException e) {
      conn.rollback();
      throw new SQLException(e);
    }
  }

  @Override
  public int executeUpdate(String cmd) throws SQLException {
    try {
      Transaction tx = conn.getTransaction();
      int result = planner.executeUpdate(cmd, tx);
      conn.commit();
      return result;
    } catch (RuntimeException e) {
      conn.rollback();
      throw new SQLException(e);
    }
  }

  @Override
  public void close() throws SQLException {
    // do nothing
  }
}
