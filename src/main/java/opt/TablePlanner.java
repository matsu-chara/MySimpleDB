package opt;

import index.planner.IndexJoinPlan;
import index.planner.IndexSelectPlan;
import java.util.Map;
import metadata.IndexInfo;
import metadata.MetadataMgr;
import multibuffer.MultibufferProductPlan;
import plan.Plan;
import plan.SelectPlan;
import plan.TablePlan;
import query.Predicate;
import record.Schema;
import tx.Transaction;

public class TablePlanner {
  private TablePlan myplan;
  private Predicate mypred;
  private Schema myschema;
  private Map<String, IndexInfo> indexes;
  private Transaction tx;

  public TablePlanner(String tbname, Predicate mypred, Transaction tx, MetadataMgr mdm) {
    this.mypred = mypred;
    this.tx = tx;
    myplan = new TablePlan(tx, tbname, mdm);
    myschema = myplan.schema();
    indexes = mdm.getIndexInfo(tbname, tx);
  }

  public Plan makeSelectPlan() {
    var p = makeIndexSelect();
    if (p == null) {
      p = myplan;
    }
    return addSelectPred(p);
  }

  public Plan makeJoinPlan(Plan current) {
    var currsch = current.schema();
    var joinpred = mypred.joinSubPred(myschema, currsch);
    if (joinpred == null) {
      return null;
    }
    var p = makeIndexJoin(current, currsch);
    if (p == null) {
      p = makeProductJoin(current, currsch);
    }
    return p;
  }

  public Plan makeProductPlan(Plan current) {
    var p = addSelectPred(myplan);
    return new MultibufferProductPlan(tx, current, p);
  }

  private Plan makeIndexSelect() {
    for (var fldname : indexes.keySet()) {
      var val = mypred.equatesWithConstant(fldname);
      if (val != null) {
        var ii = indexes.get(fldname);
        return new IndexSelectPlan(myplan, ii, val);
      }
    }
    return null;
  }

  private Plan makeIndexJoin(Plan current, Schema currsch) {
    for (var fldname : indexes.keySet()) {
      var outerfield = mypred.equatesWithField(fldname);
      if (outerfield != null & currsch.hasField(outerfield)) {
        var ii = indexes.get(fldname);
        var p = addSelectPred(new IndexJoinPlan(current, myplan, ii, outerfield));
        return addJoinPred(p, currsch);
      }
    }
    return null;
  }

  private Plan makeProductJoin(Plan current, Schema currsch) {
    var p = makeProductPlan(current);
    return addJoinPred(p, currsch);
  }

  private Plan addSelectPred(Plan p) {
    var selectpred = mypred.selectSubPred(myschema);
    if (selectpred != null) {
      return new SelectPlan(p, selectpred);
    } else {
      return p;
    }
  }

  private Plan addJoinPred(Plan p, Schema currsch) {
    var joinpred = mypred.joinSubPred(currsch, myschema);
    if (joinpred != null) {
      return new SelectPlan(p, joinpred);
    } else {
      return p;
    }
  }
}
