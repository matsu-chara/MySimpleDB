package materialize;

import java.util.Arrays;
import java.util.List;
import query.Constant;
import query.Scan;
import query.UpdateScan;
import record.RID;

public class SortScan implements Scan {
  private UpdateScan s1, s2, currentscan;
  private RecordComparator comp;
  private boolean hasmore1, hasmore2;
  private List<RID> savedpositions;

  public SortScan(List<TempTable> runs, RecordComparator comp) {
    this.comp = comp;
    this.s1 = runs.get(0).open();
    hasmore1 = s1.next();
    if (runs.size() > 1) {
      s2 = runs.get(1).open();
      hasmore2 = s2.next();
    }
  }

  @Override
  public void beforeFirst() {
    currentscan = null;
    s1.beforeFirst();
    hasmore1 = s1.next();
    if (s2 != null) {
      s2.beforeFirst();
      hasmore2 = s2.next();
    }
  }

  @Override
  public boolean next() {
    if (currentscan != null) {
      if (currentscan == s1) {
        hasmore1 = s1.next();
      } else if (s2.next()) {
        hasmore2 = s2.next();
      }
    }

    if (!hasmore1 && !hasmore2) {
      return false;
    } else if (hasmore1 && hasmore2) {
      currentscan = (comp.compare(s1, s2) < 0) ? s1 : s2;
    } else if (hasmore1) {
      currentscan = s1;
    } else {
      currentscan = s2;
    }

    return true;
  }

  @Override
  public int getInt(String fldname) {
    return currentscan.getInt(fldname);
  }

  @Override
  public String getString(String fldname) {
    return currentscan.getString(fldname);
  }

  @Override
  public Constant getVal(String fldname) {
    return currentscan.getVal(fldname);
  }

  @Override
  public boolean hasField(String fldname) {
    return currentscan.hasField(fldname);
  }

  @Override
  public void close() {
    s1.close();
    if (s2 != null) {
      s2.close();
    }
  }

  public void savePosition() {
    var rid1 = s1.getRid();
    var rid2 = (s2 == null) ? null : s2.getRid();
    savedpositions = Arrays.asList(rid1, rid2);
  }

  public void restorePosition() {
    var rid1 = savedpositions.get(0);
    var rid2 = savedpositions.get(1);
    s1.moveToRid(rid1);
    if (rid2 != null) {
      s2.moveToRid(rid2);
    }
  }
}
