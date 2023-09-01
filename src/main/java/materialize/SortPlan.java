package materialize;

import java.util.ArrayList;
import java.util.List;
import plan.Plan;
import query.Scan;
import query.UpdateScan;
import record.Schema;
import tx.Transaction;

public class SortPlan implements Plan {
  private Transaction tx;
  private Plan p;
  private Schema sch;
  private RecordComparator comp;

  public SortPlan(Transaction tx, Plan p, List<String> sortfields) {
    this.tx = tx;
    this.p = p;
    this.sch = p.schema();
    this.comp = new RecordComparator(sortfields);
  }

  @Override
  public Scan open() {
    var src = p.open();
    var runs = splitIntoRuns(src);
    src.close();
    while (runs.size() > 2) {
      runs = doAMergeIteration(runs);
    }
    return new SortScan(runs, comp);
  }

  @Override
  public int blocksAccessed() {
    var mp = new MaterializePlan(p, tx);
    return mp.blocksAccessed();
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
    return sch;
  }

  private List<TempTable> splitIntoRuns(Scan src) {
    var temps = new ArrayList<TempTable>();
    src.beforeFirst();

    if (!src.next()) {
      return temps;
    }
    var currenttemp = new TempTable(tx, sch);
    temps.add(currenttemp);
    var currentscan = currenttemp.open();
    while (copy(src, currentscan)) {
      if (comp.compare(src, currentscan) < 0) {
        // start a new run
        currentscan.close();
        currenttemp = new TempTable(tx, sch);
        temps.add(currenttemp);
        currentscan = currenttemp.open();
      }
    }
    currentscan.close();
    return temps;
  }

  private List<TempTable> doAMergeIteration(List<TempTable> runs) {
    var result = new ArrayList<TempTable>();
    while (runs.size() > 1) {
      var p1 = runs.remove(0);
      var p2 = runs.remove(0);
      result.add(mergeTwoRuns(p1, p2));
    }
    if (runs.size() == 1) {
      result.add(runs.get(0));
    }
    return result;
  }

  private TempTable mergeTwoRuns(TempTable p1, TempTable p2) {
    var src1 = p1.open();
    var src2 = p2.open();
    var result = new TempTable(tx, sch);
    var dest = result.open();

    var hasmore1 = src1.next();
    var hasmore2 = src2.next();
    while (hasmore1 && hasmore2) {
      if (comp.compare(src1, src2) < 0) {
        hasmore1 = copy(src1, dest);
      } else {
        hasmore2 = copy(src2, dest);
      }
    }

    if (hasmore1) {
      while (hasmore1) {
        hasmore1 = copy(src1, dest);
      }
    }
    if (hasmore2) {
      while (hasmore2) {
        hasmore2 = copy(src2, dest);
      }
    }
    src1.close();
    src2.close();
    dest.close();
    return result;
  }

  private boolean copy(Scan src, UpdateScan dest) {
    dest.insert();
    for (var fldname : sch.fields()) {
      var val = src.getVal(fldname);
      dest.setVal(fldname, val);
    }
    return src.next();
  }
}
