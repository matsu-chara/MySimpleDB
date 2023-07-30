package plan;

import query.ProductScan;
import query.Scan;
import record.Schema;

public class ProductPlan implements Plan {
  private Plan p1, p2;

  private Schema schema = new Schema();

  public ProductPlan(Plan p1, Plan p2) {
    this.p1 = p1;
    this.p2 = p2;
    schema.addAll(p1.schema());
    schema.addAll(p2.schema());
  }

  @Override
  public Scan open() {
    var s1 = p1.open();
    var s2 = p2.open();
    return new ProductScan(s1, s2);
  }

  @Override
  public int blocksAccessed() {
    return p1.blocksAccessed() + (p1.recordsOutput() * p2.blocksAccessed());
  }

  @Override
  public int recordsOutput() {
    return p1.recordsOutput() * p2.recordsOutput();
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
    return schema;
  }
}
