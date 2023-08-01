package plan;

import parser.QueryData;
import tx.Transaction;

public interface QueryPlanner {
  public Plan createPlan(QueryData data, Transaction tx);
}
