package materialize;

import java.util.Comparator;
import java.util.List;
import query.Scan;

public record RecordComparator(List<String> field) implements Comparator<Scan> {
  @Override
  public int compare(Scan o1, Scan o2) {
    for (var fldname : field) {
      var val1 = o1.getVal(fldname);
      var val2 = o2.getVal(fldname);
      var result = val1.compareTo(val2);
      if (result != 0) {
        return result;
      }
    }
    return 0;
  }
}
