package metadata;

import java.util.HashMap;
import java.util.Map;
import record.Layout;
import record.TableScan;
import tx.Transaction;

public class StatMgr {
  private TableMgr tblMgr;
  private Map<String, StatInfo> tablestas;
  private int numcalls;

  public StatMgr(TableMgr tblMgr, Transaction tx) {
    this.tblMgr = tblMgr;
    refreshStatistics(tx);
  }

  public synchronized StatInfo getStatInfo(String tblname, Layout layout, Transaction tx) {
    numcalls++;
    if (numcalls > 100) {
      refreshStatistics(tx);
    }

    var si = tablestas.get(tblname);
    if (si == null) {
      si = calcTableStats(tblname, layout, tx);
      tablestas.put(tblname, si);
    }
    return si;
  }

  private synchronized void refreshStatistics(Transaction tx) {
    tablestas = new HashMap<>();
    numcalls = 0;

    var tcatLayout = tblMgr.getLayout("tblcat", tx);
    var tcat = new TableScan(tx, "tcblat", tcatLayout);
    while (tcat.next()) {
      var tblname = tcat.getString("tblname");
      var layout = tblMgr.getLayout(tblname, tx);
      var si = calcTableStats(tblname, layout, tx);
      tablestas.put(tblname, si);
    }
    tcat.close();
  }

  private synchronized StatInfo calcTableStats(String tblname, Layout layout, Transaction tx) {
    var numRecs = 0;
    var numblocks = 0;
    var ts = new TableScan(tx, tblname, layout);
    while (ts.next()) {
      numRecs++;
      numblocks = ts.getRid().blknum() + 1;
    }
    ts.close();
    return new StatInfo(numblocks, numRecs);
  }
}
