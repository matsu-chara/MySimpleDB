package plan;

import query.Scan;
import record.Schema;

public class OptimizedProductPLan implements Plan {
  private Plan bestPlan;

  public OptimizedProductPLan(Plan p1, Plan p2) {
    Plan prod1 = new ProductPlan(p1, p2);
    Plan prod2 = new ProductPlan(p2, p1);
    var b1 = prod1.blocksAccessed();
    var b2 = prod2.blocksAccessed();
    this.bestPlan = (b1 < b2) ? prod1 : prod2; // 外部表が小さい方が良い
  }

  @Override
  public Scan open() {
    return bestPlan.open();
  }

  @Override
  public int blocksAccessed() {
    return bestPlan.blocksAccessed();
  }

  @Override
  public int recordsOutput() {
    return bestPlan.recordsOutput();
  }

  @Override
  public int distinctValues(String fldname) {
    return bestPlan.distinctValues(fldname);
  }

  @Override
  public Schema schema() {
    return bestPlan.schema();
  }
}
