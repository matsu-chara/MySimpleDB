package opt;

import java.util.ArrayList;
import java.util.Collection;
import metadata.MetadataMgr;
import parser.QueryData;
import plan.Plan;
import plan.Planner;
import plan.ProjectPlan;
import plan.QueryPlanner;
import tx.Transaction;

public class HeuristicQueryPlanner implements QueryPlanner {
  private Collection<TablePlanner> tableplannners = new ArrayList<>();
  private MetadataMgr mdm;

  public HeuristicQueryPlanner(MetadataMgr mdm) {
    this.mdm = mdm;
  }

  @Override
  public Plan createPlan(QueryData data, Transaction tx) {
    data.tables().stream()
        .map(tblname -> new TablePlanner(tblname, data.pred(), tx, mdm))
        .forEach(tp -> tableplannners.add(tp));

    var currentplan = getLowestSelectPlan();

    while (!tableplannners.isEmpty()) {
      var p = getLowestJoinPlan(currentplan);
      if (p != null) {
        currentplan = p;
      } else {
        currentplan = getLowestProductPlan(currentplan);
      }
    }

    return new ProjectPlan(currentplan, data.fields());
  }

  private Plan getLowestSelectPlan() {
    TablePlanner besttp = null;
    Plan bestplan = null;
    for (var tp : tableplannners) {
      var plan = tp.makeSelectPlan();
      if (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput()) {
        besttp = tp;
        bestplan = plan;
      }
    }
    tableplannners.remove(besttp);
    return bestplan;
  }

  private Plan getLowestJoinPlan(Plan current) {
    TablePlanner besttp = null;
    Plan bestplan = null;
    for (var tp : tableplannners) {
      var plan = tp.makeJoinPlan(current);
      if (plan != null && (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput())) {
        besttp = tp;
        bestplan = plan;
      }
    }
    if (bestplan != null) {
      tableplannners.remove(besttp);
    }
    return bestplan;
  }

  private Plan getLowestProductPlan(Plan current) {
    TablePlanner besttp = null;
    Plan bestplan = null;
    for (var tp : tableplannners) {
      var plan = tp.makeProductPlan(current);
      if (bestplan == null || plan.recordsOutput() < bestplan.recordsOutput()) {
        besttp = tp;
        bestplan = plan;
      }
    }
    tableplannners.remove(besttp);
    return bestplan;
  }

  public void setPlanner(Planner p) {
    // do nothing for simplicity
  }
}
