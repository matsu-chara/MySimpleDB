package multibuffer;

import materialize.MaterializePlan;
import materialize.TempTable;
import plan.Plan;
import query.Scan;
import record.Schema;
import tx.Transaction;

public class MultibufferProductPlan implements Plan {
  private Transaction tx;
  private Plan lhs, rhs;
  private Schema schema = new Schema();

  public MultibufferProductPlan(Transaction tx, Plan lhs, Plan rhs) {
    this.tx = tx;
    this.lhs = new MaterializePlan(lhs, tx);
    this.rhs = rhs;
    schema.addAll(lhs.schema());
    schema.addAll(rhs.schema());
  }

  @Override
  public Scan open() {
    var leftscan = lhs.open();
    var tt = copyRecordsFrom(rhs);
    return new MultibufferProductScan(tx, leftscan, tt.tblname(), tt.getLayout());
  }

  @Override
  public int blocksAccessed() {
    var avail = tx.availableBuffers();
    var size = new MaterializePlan(rhs, tx).blocksAccessed();
    var numchunks = size / avail;
    return rhs.blocksAccessed() + (lhs.blocksAccessed() * numchunks);
  }

  @Override
  public int recordsOutput() {
    return lhs.recordsOutput() * rhs.recordsOutput();
  }

  @Override
  public int distinctValues(String fldname) {
    if (lhs.schema().hasField(fldname)) {
      return lhs.distinctValues(fldname);
    } else {
      return rhs.distinctValues(fldname);
    }
  }

  @Override
  public Schema schema() {
    return schema;
  }

  private TempTable copyRecordsFrom(Plan p) {
    var src = p.open();
    var sch = p.schema();
    var t = new TempTable(tx, sch);
    var dest = t.open();
    while (src.next()) {
      dest.insert();
      sch.fields().forEach(fldname -> dest.setVal(fldname, src.getVal(fldname)));
    }
    src.close();
    dest.close();
    return t;
  }
}
