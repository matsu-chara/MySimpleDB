package index.planner;

import index.query.IndexJoinScan;
import metadata.IndexInfo;
import plan.Plan;
import query.Scan;
import record.Schema;
import record.TableScan;

public class IndexJoinPlan implements Plan {
  private Plan p1, p2;
  private IndexInfo ii;
  private String joinfield;
  private Schema sch = new Schema();

  public IndexJoinPlan(Plan p1, Plan p2, IndexInfo ii, String joinfield) {
    this.p1 = p1;
    this.p2 = p2;
    this.ii = ii;
    this.joinfield = joinfield;
    sch.addAll(p1.schema());
    sch.addAll(p2.schema());
  }

  @Override
  public Scan open() {
    var s = p1.open();
    var ts = (TableScan) p2.open();
    var idx = ii.open();
    return new IndexJoinScan(s, idx, joinfield, ts);
  }

  @Override
  public int blocksAccessed() {
    return p1.blocksAccessed() + (p1.recordsOutput() + ii.recordsOutput()) + recordsOutput();
  }

  @Override
  public int recordsOutput() {
    return p1.recordsOutput() + ii.recordsOutput();
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
