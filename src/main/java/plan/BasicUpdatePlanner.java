package plan;

import metadata.MetadataMgr;
import parser.CreateIndexData;
import parser.CreateTableData;
import parser.CreateViewData;
import parser.DeleteData;
import parser.ModifyData;
import query.UpdateScan;
import tx.Transaction;

public class BasicUpdatePlanner implements UpdatePlanner {
  private MetadataMgr mdm;

  public BasicUpdatePlanner(MetadataMgr mdm) {
    this.mdm = mdm;
  }

  @Override
  public int executeDelete(DeleteData data, Transaction tx) {
    Plan p = new TablePlan(tx, data.tblname(), mdm);
    p = new SelectPlan(p, data.pred());
    var us = (UpdateScan) p.open();
    var count = 0;
    while (us.next()) {
      us.delete();
      count++;
    }
    us.close();
    return count;
  }

  @Override
  public int executeModify(ModifyData data, Transaction tx) {
    Plan p = new TablePlan(tx, data.tblname(), mdm);
    p = new SelectPlan(p, data.pred());
    var us = (UpdateScan) p.open();
    var count = 0;
    while (us.next()) {
      var val = data.newval().evaluate(us);
      us.setVal(data.targetField(), val);
      count++;
    }
    us.close();
    return count;
  }

  @Override
  public int executeCreateTable(CreateTableData data, Transaction tx) {
    mdm.createTable(data.tblname(), data.newSchema(), tx);
    return 0;
  }

  @Override
  public int executeCreateView(CreateViewData data, Transaction tx) {
    mdm.createView(data.viewname(), data.viewDef(), tx);
    return 0;
  }

  @Override
  public int executeIndex(CreateIndexData data, Transaction tx) {
    mdm.createIndex(data.idxname(), data.tblname(), data.fldname(), tx);
    return 0;
  }
}
