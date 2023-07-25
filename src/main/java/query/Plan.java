package query;

import record.Schema;

public interface Plan {
  Scan open();

  int blocksAccessed();

  int recordsOutput();

  int distinctValues(String fldname);

  Schema schema();
}
