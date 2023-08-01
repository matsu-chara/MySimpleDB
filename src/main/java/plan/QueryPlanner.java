package plan;

import parser.QueryData;
import tx.Transaction;

public interface QueryPlanner {
  Plan createPlan(QueryData data, Transaction tx);
}
