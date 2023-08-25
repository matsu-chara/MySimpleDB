package metadata;

import java.util.Map;
import record.Layout;
import record.Schema;
import tx.Transaction;

public class MetadataMgr {
  private static TableMgr tblmgr;
  private static ViewMgr viewMgr;
  private static StatMgr statmgr;
  private static IndexMgr idxmgr;

  public MetadataMgr(boolean isNew, Transaction tx, IndexInfo.IndexMode indexMode) {
    tblmgr = new TableMgr(isNew, tx);
    viewMgr = new ViewMgr(isNew, tblmgr, tx);
    statmgr = new StatMgr(tblmgr, tx);
    idxmgr = new IndexMgr(isNew, tblmgr, statmgr, tx, indexMode);
  }

  public void createTable(String tblname, Schema sch, Transaction tx) {
    tblmgr.createTable(tblname, sch, tx);
  }

  public Layout getLayout(String tblname, Transaction tx) {
    return tblmgr.getLayout(tblname, tx);
  }

  public void createView(String viewname, String viewdef, Transaction tx) {
    viewMgr.createView(viewname, viewdef, tx);
  }

  public String getViewDef(String viewname, Transaction tx) {
    return viewMgr.getView(viewname, tx);
  }

  public void createIndex(String idxname, String tblname, String fldname, Transaction tx) {
    idxmgr.createIndex(idxname, tblname, fldname, tx);
  }

  public Map<String, IndexInfo> getIndexInfo(String tblname, Transaction tx) {
    return idxmgr.getIndexInfo(tblname, tx);
  }

  public StatInfo getStatInfo(String tblname, Layout layout, Transaction tx) {
    return statmgr.getStatInfo(tblname, layout, tx);
  }
}
