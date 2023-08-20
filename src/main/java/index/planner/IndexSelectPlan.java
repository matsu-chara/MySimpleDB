package index.planner;

import index.query.IndexSelectScan;
import metadata.IndexInfo;
import plan.Plan;
import query.Constant;
import query.Scan;
import record.Schema;
import record.TableScan;

public class IndexSelectPlan implements Plan {

  private Plan p;
  private IndexInfo ii;
  private Constant val;

  public IndexSelectPlan(Plan p, IndexInfo ii, Constant val) {
    this.p = p;
    this.ii = ii;
    this.val = val;
  }

  @Override
  public Scan open() {
    var ts = (TableScan) p.open();
    var idx = ii.open();
    return new IndexSelectScan(ts, idx, val);
  }

  @Override
  public int blocksAccessed() {
    return ii.blocksAccessed() + recordsOutput();
  }

  @Override
  public int recordsOutput() {
    return ii.recordsOutput();
  }

  @Override
  public int distinctValues(String fldname) {
    return ii.distinctValues(fldname);
  }

  @Override
  public Schema schema() {
    return p.schema();
  }
}
