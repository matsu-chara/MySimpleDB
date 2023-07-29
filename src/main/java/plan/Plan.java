package plan;

import query.Scan;

public interface Plan {
  public Scan open();
}
