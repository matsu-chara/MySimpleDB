package materialize;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import query.Constant;
import query.Scan;

public class GroupValue {
  private Map<String, Constant> vals = new HashMap<>();

  public GroupValue(Scan s, List<String> fields) {
    for (var fldname : fields) {
      vals.put(fldname, s.getVal(fldname));
    }
  }

  public Constant getVal(String fldname) {
    return vals.get(fldname);
  }

  public boolean equals(Object obj) {
    if (getClass() != obj.getClass()) {
      return false;
    }

    var gv = (GroupValue) obj;
    for (var fldname : vals.keySet()) {
      var c1 = vals.get(fldname);
      var c2 = gv.getVal(fldname);
      if (!c1.equals(c2)) {
        return false;
      }
    }
    return true;
  }

  public int hashCode() {
    var hashval = vals.values().stream().mapToInt(Constant::hashCode).sum();
    return hashval;
  }
}
