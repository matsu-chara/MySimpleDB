package plan;

import java.util.ArrayList;
import java.util.List;
import metadata.MetadataMgr;
import parser.Parser;
import parser.QueryData;
import tx.Transaction;

public class BasicQueryPlanner implements QueryPlanner {
  private MetadataMgr mdm;

  public BasicQueryPlanner(MetadataMgr mdm) {
    this.mdm = mdm;
  }

  @Override
  public Plan createPlan(QueryData data, Transaction tx) {
    // 参照されているすべてのテーブル・ビューへのPLanを作る
    List<Plan> plans = new ArrayList<>();
    for (var tblname : data.tables()) {
      var viewdef = mdm.getViewDef(tblname, tx);
      if (viewdef != null) { // 再帰でview用のPlanをとってくる
        var parser   = new Parser(viewdef);
        var viewdata = parser.query();
        plans.add(createPlan(viewdata, tx));
      } else {
        plans.add(new TablePlan(tblname, tx, mdm));
      }
    }

    // Product
    var p = plans.remove(0);
    for (var nextPlan : plans) {
      p = new ProductPlan(p, nextPlan);
    }

    // Select
    p = new SelectPlan(p, data.pred());

    // Project
    p = new ProjectPlan(p, data.fields());
    return p;
  }
}
