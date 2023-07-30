package plan;

import java.util.List;
import query.ProjectScan;
import query.Scan;
import record.Schema;

public class ProjectPlan implements Plan {
  private Plan p;
  private Schema schema = new Schema();

  public ProjectPlan(Plan p, List<String> fieldList) {
    this.p = p;
    for (var fldName : fieldList) {
      schema.add(fldName, p.schema());
    }
  }

  @Override
  public Scan open() {
    var s = p.open();
    return new ProjectScan(s, schema.fields());
  }

  @Override
  public int blocksAccessed() {
    return p.blocksAccessed();
  }

  @Override
  public int recordsOutput() {
    return p.recordsOutput();
  }

  @Override
  public int distinctValues(String fldname) {
    return p.distinctValues(fldname);
  }

  @Override
  public Schema schema() {
    return schema;
  }
}
