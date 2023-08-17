package index.has;

import index.Index;
import query.Constant;
import record.Layout;
import record.RID;
import record.TableScan;
import tx.Transaction;

public class HashIndex implements Index {
  public static int NUM_BUCKETS = 100;
  private Transaction tx;
  private String idxname;
  private Layout layout;
  private Constant searchkey = null;
  private TableScan ts = null;

  public HashIndex(Transaction tx, String idxname, Layout layout) {
    this.tx = tx;
    this.idxname = idxname;
    this.layout = layout;
  }

  @Override
  public void beforeFirst(Constant searchkey) {
    close();
    this.searchkey = searchkey;
    var bucket = searchkey.hashCode() % NUM_BUCKETS;
    var tblname = idxname + bucket;
    ts = new TableScan(tx, tblname, layout);
  }

  @Override
  public boolean next() {
    while (ts.next()) {
      if (ts.getVal("dataval").equals(searchkey)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public RID getRid() {
    var blknum = ts.getInt("block");
    var id = ts.getInt("id");
    return new RID(blknum, id);
  }

  @Override
  public void insert(Constant dataval, RID datarid) {
    beforeFirst(dataval);
    ts.insert();
    ts.setInt("block", datarid.blknum());
    ts.setInt("id", datarid.slot());
    ts.setVal("dataval", dataval);
  }

  @Override
  public void delete(Constant dataval, RID datarid) {
    beforeFirst(dataval);
    while (next()) {
      if (getRid().equals(datarid)) {
        ts.delete();
        return;
      }
    }
  }

  @Override
  public void close() {
    if (ts != null) {
      ts.close();
    }
  }
}
