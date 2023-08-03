package plan;

import parser.CreateIndexData;
import parser.CreateTableData;
import parser.CreateViewData;
import parser.DeleteData;
import parser.InsertData;
import parser.ModifyData;
import parser.Parser;
import parser.QueryData;
import tx.Transaction;

public class Planner {
  private QueryPlanner qplanner;
  private UpdatePlanner uplanner;

  public Planner(QueryPlanner qplanner, UpdatePlanner uplanner) {
    this.qplanner = qplanner;
    this.uplanner = uplanner;
  }

  public Plan createQueryPlan(String qry, Transaction tx) {
    var parser = new Parser(qry);
    var data = parser.query();
    verifyQuery(data);
    return qplanner.createPlan(data, tx);
  }

  public int executeUpdate(String cmd, Transaction tx) {
    var parser = new Parser(cmd);
    var data = parser.updateCmd();
    verifyUpdate(data);

    if (data instanceof InsertData) {
      return uplanner.executeInsert((InsertData) data, tx);
    } else if (data instanceof DeleteData) {
      return uplanner.executeDelete((DeleteData) data, tx);
    } else if (data instanceof ModifyData) {
      return uplanner.executeModify((ModifyData) data, tx);
    } else if (data instanceof CreateTableData) {
      return uplanner.executeCreateTable((CreateTableData) data, tx);
    } else if (data instanceof CreateViewData) {
      return uplanner.executeCreateView((CreateViewData) data, tx);
    } else if (data instanceof CreateIndexData) {
      return uplanner.executeCreateIndex((CreateIndexData) data, tx);
    } else {
      return 0;
    }
  }

  public void verifyQuery(QueryData data) {
    // simpledbではなにもしない
  }

  public void verifyUpdate(Object data) {
    // simpledbではなにもしない
  }
}
