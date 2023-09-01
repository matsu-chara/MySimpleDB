package materialize;

import java.util.List;
import query.Constant;
import query.Scan;

public class GroupByScan implements Scan {
  private Scan s;
  private List<String> groupfields;
  private List<AggregationFn> aggfns;
  private GroupValue groupval;
  private boolean moregroups;

  public GroupByScan(Scan s, List<String> groupfields, List<AggregationFn> aggfns) {
    this.s = s;
    this.groupfields = groupfields;
    this.aggfns = aggfns;
    beforeFirst();
  }

  @Override
  public void beforeFirst() {
    s.beforeFirst();
    moregroups = s.next();
  }

  @Override
  public boolean next() {
    if (!moregroups) {
      return false;
    }
    for (var fn : aggfns) {
      fn.processFirst(s);
    }
    groupval = new GroupValue(s, groupfields);

    while (moregroups) {
      moregroups = s.next();
      var gv = new GroupValue(s, groupfields);
      if (!groupval.equals(gv)) {
        break;
      }
      for (var fn : aggfns) {
        fn.processNext(s);
      }
    }
    return true;
  }

  @Override
  public int getInt(String fldname) {
    return getVal(fldname).asInt();
  }

  @Override
  public String getString(String fldname) {
    return getVal(fldname).asString();
  }

  @Override
  public Constant getVal(String fldname) {
    if (groupfields.contains(fldname)) {
      return groupval.getVal(fldname);
    }
    for (var fn : aggfns) {
      if (fn.fieldName().equals(fldname)) {
        return fn.value();
      }
    }
    throw new RuntimeException("field " + fldname + " not found.");
  }

  @Override
  public boolean hasField(String fldname) {
    if (groupfields.contains(fldname)) {
      return true;
    }
    return aggfns.stream().anyMatch(fn -> fn.fieldName().equals(fldname));
  }

  @Override
  public void close() {
    s.close();
  }
}
