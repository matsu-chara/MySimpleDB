package metadata;

import record.Schema;
import record.TableScan;
import tx.Transaction;

public class ViewMgr {
  private static final int MAX_VIEWDEF = 100;

  TableMgr tblMgr;

  public ViewMgr(boolean isNew, TableMgr tblMgr, Transaction tx) {
    this.tblMgr = tblMgr;
    if (isNew) {
      var sch = new Schema();
      sch.addStringField("viewname", MAX_VIEWDEF);
      sch.addStringField("viewdef", MAX_VIEWDEF);
      tblMgr.createTable("viewcat", sch, tx);
    }
  }

  public void createView(String vname, String vdef, Transaction tx) {
    var layout = tblMgr.getLayout("viewcat", tx);
    var ts = new TableScan(tx, "viewcat", layout);
    ts.insert();
    ts.setString("viewname", vname);
    ts.setString("viewdef", vdef);
    ts.close();
  }

  public String getView(String vname, Transaction tx) {
    String result = null;
    var layout = tblMgr.getLayout("viewcat", tx);
    var ts = new TableScan(tx, "viewcat", layout);
    while (ts.next()) {
      if (ts.getString("viewname").equals(vname)) {
        result = ts.getString("viewdef");
        break;
      }
    }
    ts.close();
    return result;
  }
}
