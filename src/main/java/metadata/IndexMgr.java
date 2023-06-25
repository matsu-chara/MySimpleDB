package metadata;

import static metadata.TableMgr.MAX_NAME;

import java.util.HashMap;
import java.util.Map;
import record.Layout;
import record.Schema;
import record.TableScan;
import tx.Transaction;

public class IndexMgr {
  private Layout layout;
  private TableMgr tblmgr;
  private StatMgr statmgr;

  public IndexMgr(boolean isNew, TableMgr tblmgr, StatMgr statmgr, Transaction tx) {
    if (isNew) {
      var sch = new Schema();
      sch.addStringField("indexname", MAX_NAME);
      sch.addStringField("tablename", MAX_NAME);
      sch.addStringField("fieldname", MAX_NAME);
      tblmgr.createTable("idxcat", sch, tx);
    }

    this.tblmgr = tblmgr;
    this.statmgr = statmgr;
    layout = tblmgr.getLayout("idxcat", tx);
  }

  public void createIndex(String idxname, String tblname, String fldname, Transaction tx) {
    var ts = new TableScan(tx, "idxcat", layout);
    ts.insert();
    ts.setString("indexname", idxname);
    ts.setString("tablename", tblname);
    ts.setString("fieldname", fldname);
    ts.close();
  }

  public Map<String, IndexInfo> getIndexInfo(String tblname, Transaction tx) {
    Map<String, IndexInfo> result = new HashMap<>();
    var ts = new TableScan(tx, "idxcat", layout);
    while (ts.next()) {
      if (ts.getString("tablename").equals(tblname)) {
        var idxname = ts.getString("indexname");
        var fldname = ts.getString("fieldname");
        var tblLayout = tblmgr.getLayout(tblname, tx);
        var tblsi = statmgr.getStatInfo(tblname, tblLayout, tx);
        var ii = new IndexInfo(idxname, fldname, tx, tblLayout.schema(), tblsi);
        result.put(fldname, ii);
      }
    }
    ts.close();
    return result;
  }
}
