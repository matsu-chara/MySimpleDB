package materialize;

import query.UpdateScan;
import record.Layout;
import record.Schema;
import record.TableScan;
import tx.Transaction;

public class TempTable {
  private static int nextTableNum = 0;
  private Transaction tx;
  private String tblname;
  private Layout layout;

  public TempTable(Transaction tx, Schema sch) {
    this.tx = tx;
    this.tblname = nextTableName();
    this.layout = new Layout(sch);
  }

  public UpdateScan open() {
    return new TableScan(tx, tblname, layout);
  }

  public String tblname() {
    return tblname;
  }

  public Layout getLayout() {
    return layout;
  }

  private String nextTableName() {
    nextTableNum++;
    return "temp" + nextTableNum;
  }
}
