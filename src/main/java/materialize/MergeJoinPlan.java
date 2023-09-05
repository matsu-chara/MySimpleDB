package materialize;

import java.util.List;
import plan.Plan;
import query.Scan;
import record.Schema;
import tx.Transaction;

public class MergeJoinPlan implements Plan {
  private Plan p1, p2;
  private String fldname1, fldname2;
  private Schema sch = new Schema();

  public MergeJoinPlan(Transaction tx, Plan p1, Plan p2, String fldname1, String fldname2) {
    this.fldname1 = fldname1;
    this.fldname2 = fldname2;

    this.p1 = new SortPlan(tx, p1, List.of(fldname1));
    this.p2 = new SortPlan(tx, p2, List.of(fldname2));

    sch.addAll(p1.schema());
    sch.addAll(p2.schema());
  }

  @Override
  public Scan open() {
    var s1 = p1.open();
    var s2 = (SortScan) p2.open();
    return new MergeJoinScan(s1, s2, fldname1, fldname2);
  }

  @Override
  public int blocksAccessed() {
    return p1.blocksAccessed() + p2.blocksAccessed();
  }

  @Override
  public int recordsOutput() {
    var maxVals = Math.max(p1.distinctValues(fldname1), p2.distinctValues(fldname2));
    return (p1.recordsOutput() * p2.recordsOutput()) / maxVals;
  }

  @Override
  public int distinctValues(String fldname) {
    if (p1.schema().hasField(fldname)) {
      return p1.distinctValues(fldname);
    } else {
      return p2.distinctValues(fldname);
    }
  }

  @Override
  public Schema schema() {
    return sch;
  }
}
