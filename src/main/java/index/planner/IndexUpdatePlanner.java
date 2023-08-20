package index.planner;

import metadata.MetadataMgr;
import parser.CreateIndexData;
import parser.CreateTableData;
import parser.CreateViewData;
import parser.DeleteData;
import parser.InsertData;
import parser.ModifyData;
import plan.Plan;
import plan.SelectPlan;
import plan.TablePlan;
import plan.UpdatePlanner;
import query.UpdateScan;
import tx.Transaction;

public class IndexUpdatePlanner implements UpdatePlanner {
  private MetadataMgr mdm;

  public IndexUpdatePlanner(MetadataMgr mdm) {
    this.mdm = mdm;
  }

  @Override
  public int executeInsert(InsertData data, Transaction tx) {
    var tblname = data.tblname();
    Plan p = new TablePlan(tx, tblname, mdm);

    var s = (UpdateScan) p.open();
    s.insert();
    var rid = s.getRid();

    var indexes = mdm.getIndexInfo(tblname, tx);
    var valIter = data.vals().iterator();
    for (var fldname : data.fields()) {
      var val = valIter.next();
      s.setVal(fldname, val);

      // update index
      var ii = indexes.get(fldname);
      if (ii != null) {
        var idx = ii.open();
        idx.insert(val, rid);
        idx.close();
      }
    }
    s.close();
    return 1;
  }

  @Override
  public int executeDelete(DeleteData data, Transaction tx) {
    var tblname = data.tblname();
    Plan p = new TablePlan(tx, tblname, mdm);
    p = new SelectPlan(p, data.pred());
    var indexes = mdm.getIndexInfo(tblname, tx);

    var s = (UpdateScan) p.open();
    var count = 0;
    while (s.next()) {
      var rid = s.getRid();
      for (var fldname : indexes.keySet()) {
        var val = s.getVal(fldname);
        var idx = indexes.get(fldname).open();
        idx.delete(val, rid);
        idx.close();
      }
      s.delete();
      count++;
    }
    s.close();
    return count;
  }

  @Override
  public int executeModify(ModifyData data, Transaction tx) {
    var tblname = data.tblname();
    var fldname = data.targetField();
    Plan p = new TablePlan(tx, tblname, mdm);
    p = new SelectPlan(p, data.pred());

    var ii = mdm.getIndexInfo(tblname, tx).get(fldname);
    var idx = (ii == null) ? null : ii.open();

    var s = (UpdateScan) p.open();
    var count = 0;
    while (s.next()) {
      var newval = data.newval().evaluate(s);
      var oldval = s.getVal(fldname);
      s.setVal(fldname, newval);

      if (idx != null) {
        var rid = s.getRid();
        idx.delete(oldval, rid);
        idx.insert(newval, rid);
      }
      count++;
    }
    if (idx != null) idx.close();
    s.close();
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
  public int executeCreateIndex(CreateIndexData data, Transaction tx) {
    mdm.createIndex(data.idxname(), data.tblname(), data.fldname(), tx);
    return 0;
  }
}
