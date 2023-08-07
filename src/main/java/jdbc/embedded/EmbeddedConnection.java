package jdbc.embedded;

import java.sql.SQLException;
import jdbc.ConnectionAdapter;
import plan.Planner;
import server.SimpleDB;
import tx.Transaction;

public class EmbeddedConnection extends ConnectionAdapter {
  private SimpleDB db;
  private Transaction currentTx;
  private Planner planner;

  public EmbeddedConnection(SimpleDB db) {
    this.db = db;
    currentTx = db.newTx();
    planner = db.planner();
  }

  @Override
  public EmbeddedStatement createStatement() throws SQLException {
    return new EmbeddedStatement(this, planner);
  }

  @Override
  public void close() throws SQLException {
    currentTx.commit();
  }

  @Override
  public void commit() throws SQLException {
    currentTx.commit();
    currentTx = db.newTx();
  }

  @Override
  public void rollback() throws SQLException {
    currentTx.rollback();
    currentTx = db.newTx();
  }

  Transaction getTransaction() {
    return currentTx;
  }
}
