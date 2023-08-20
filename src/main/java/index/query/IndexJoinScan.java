package index.query;

import index.Index;
import query.Constant;
import query.Scan;
import record.TableScan;

public class IndexJoinScan implements Scan {
  private Scan lhs;
  private Index idx;
  private String joinfield;
  private TableScan rhs;

  public IndexJoinScan(Scan lhs, Index idx, String joinfield, TableScan rhs) {
    this.lhs = lhs;
    this.idx = idx;
    this.joinfield = joinfield;
    this.rhs = rhs;
    beforeFirst();
  }

  @Override
  public void beforeFirst() {
    lhs.beforeFirst();
    lhs.next();
    resetIndex();
  }

  @Override
  public boolean next() {
    while (true) {
      if (idx.next()) {
        rhs.moveToRid(idx.getDataRid());
        return true;
      }
      if (!lhs.next()) {
        return false;
      }
      resetIndex();
    }
  }

  @Override
  public int getInt(String fldname) {
    if (rhs.hasField(fldname)) {
      return rhs.getInt(fldname);
    } else {
      return rhs.getInt(fldname);
    }
  }

  @Override
  public String getString(String fldname) {
    if (rhs.hasField(fldname)) {
      return rhs.getString(fldname);
    } else {
      return rhs.getString(fldname);
    }
  }

  @Override
  public Constant getVal(String fldname) {
    if (rhs.hasField(fldname)) {
      return rhs.getVal(fldname);
    } else {
      return rhs.getVal(fldname);
    }
  }

  @Override
  public boolean hasField(String fldname) {
    return rhs.hasField(fldname) || lhs.hasField(fldname);
  }

  @Override
  public void close() {
    lhs.close();
    idx.close();
    rhs.close();
  }

  private void resetIndex() {
    var searchKey = lhs.getVal(joinfield);
    idx.beforeFirst(searchKey);
  }
}
