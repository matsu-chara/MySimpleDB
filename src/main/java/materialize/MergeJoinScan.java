package materialize;

import query.Constant;
import query.Scan;

public class MergeJoinScan implements Scan {
  private Scan s1;
  private SortScan s2;
  private String fldname1, fldname2;
  private Constant joinval = null;

  public MergeJoinScan(Scan s1, SortScan s2, String fldname1, String fldname2) {
    this.s1 = s1;
    this.s2 = s2;
    this.fldname1 = fldname1;
    this.fldname2 = fldname2;
    beforeFirst();
  }

  @Override
  public void beforeFirst() {
    s1.beforeFirst();
    s2.beforeFirst();
  }

  @Override
  public boolean next() {
    var hasmore2 = s2.next();
    if (hasmore2 && s2.getVal(fldname2).equals(joinval)) {
      return true;
    }

    var hasmore1 = s1.next();
    if (hasmore1 && s1.getVal(fldname1).equals(joinval)) {
      s2.restorePosition();
      return true;
    }

    while (hasmore1 && hasmore2) {
      var v1 = s1.getVal(fldname1);
      var v2 = s2.getVal(fldname2);
      if (v1.compareTo(v2) < 0) {
        hasmore1 = s1.next();
      } else if (v1.compareTo(v2) > 0) {
        hasmore2 = s2.next();
      } else {
        s2.restorePosition();
        joinval = s2.getVal(fldname2);
        return true;
      }
    }
    return false;
  }

  @Override
  public int getInt(String fldname) {
    if (s1.hasField(fldname)) {
      return s1.getInt(fldname);
    } else {
      return s2.getInt(fldname);
    }
  }

  @Override
  public String getString(String fldname) {
    if (s1.hasField(fldname)) {
      return s1.getString(fldname);
    } else {
      return s2.getString(fldname);
    }
  }

  @Override
  public Constant getVal(String fldname) {
    if (s1.hasField(fldname)) {
      return s1.getVal(fldname);
    } else {
      return s2.getVal(fldname);
    }
  }

  @Override
  public boolean hasField(String fldname) {
    return s1.hasField(fldname) || s2.hasField(fldname);
  }

  @Override
  public void close() {
    s1.close();
    s2.close();
  }
}
