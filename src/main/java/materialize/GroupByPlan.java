package materialize;

import java.util.List;
import plan.Plan;
import query.Scan;
import record.Schema;
import tx.Transaction;

public class GroupByPlan implements Plan {
  private Plan p;
  private List<String> groupfields;
  private List<AggregationFn> aggfns;
  private Schema sch = new Schema();

  public GroupByPlan(Transaction tx, Plan p, List<String> groupfields, List<AggregationFn> aggfns) {
    this.p = new SortPlan(tx, p, groupfields);
    this.groupfields = groupfields;
    this.aggfns = aggfns;
    for (var fldname : groupfields) {
      sch.add(fldname, p.schema());
    }
    for (var fn : aggfns) {
      sch.addIntField(fn.fieldName());
    }
  }

  @Override
  public Scan open() {
    var s = p.open();
    return new GroupByScan(s, groupfields, aggfns);
  }

  @Override
  public int blocksAccessed() {
    return p.blocksAccessed();
  }

  @Override
  public int recordsOutput() {
    var numgroups =
        groupfields.stream()
            .mapToInt(fldname -> p.distinctValues(fldname))
            .reduce(1, (a, b) -> a * b);
    return numgroups;
  }

  @Override
  public int distinctValues(String fldname) {
    if (p.schema().hasField(fldname)) {
      return p.distinctValues(fldname);
    } else {
      return recordsOutput();
    }
  }

  @Override
  public Schema schema() {
    return sch;
  }
}
