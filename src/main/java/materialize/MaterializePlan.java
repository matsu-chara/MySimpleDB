package materialize;

import plan.Plan;
import query.Scan;
import record.Layout;
import record.Schema;
import tx.Transaction;

public class MaterializePlan implements Plan {
  private Plan srcplan;
  private Transaction tx;

  public MaterializePlan(Plan srcplan, Transaction tx) {
    this.srcplan = srcplan;
    this.tx = tx;
  }

  @Override
  public Scan open() {
    var sch = srcplan.schema();
    var temp = new TempTable(tx, sch);
    var src = srcplan.open();
    var dest = temp.open();
    while (src.next()) {
      dest.insert();
      for (var fldname : sch.fields()) {
        dest.setVal(fldname, src.getVal(fldname));
      }
    }
    src.close();
    dest.beforeFirst();
    return dest;
  }

  @Override
  public int blocksAccessed() {
    var layout = new Layout(srcplan.schema());
    var rpb = tx.blockSize() / layout.slotsize();
    return (int) Math.ceil((double) srcplan.recordsOutput() / rpb);
  }

  @Override
  public int recordsOutput() {
    return srcplan.recordsOutput();
  }

  @Override
  public int distinctValues(String fldname) {
    return srcplan.distinctValues(fldname);
  }

  @Override
  public Schema schema() {
    return srcplan.schema();
  }
}
