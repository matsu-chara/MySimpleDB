package index.query;

import static org.junit.jupiter.api.Assertions.*;

import index.planner.IndexJoinPlan;
import metadata.IndexInfo;
import metadata.MetadataMgr;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import plan.Plan;
import plan.TablePlan;
import record.Schema;
import record.TableScan;
import server.SimpleDB;

class IndexJoinTest {
  private SimpleDB db;
  private MetadataMgr mdm;

  @BeforeEach
  void setup() {
    db = new SimpleDB("test_scan1", 400, 8);
    mdm = db.mdMgr();

    var tx = db.newTx();

    var sch1 = new Schema();
    sch1.addIntField("sid");
    sch1.addStringField("sname", 6);
    mdm.createTable("student", sch1, tx);

    var sch2 = new Schema();
    sch2.addIntField("sid");
    sch2.addStringField("grade", 6);
    mdm.createTable("enroll", sch2, tx);
    mdm.createIndex("studentid", "enroll", "sid", tx);

    tx.commit();
  }

  @AfterEach
  void teardown() {
    db.deleteDB();
  }

  @Test
  void test() {
    var tx = db.newTx();
    for (var i = 0; i < 2; i++) {
      db.planner()
          .executeUpdate(
              "insert into student(sid, sname) values(" + i + ", 'sname_" + i + "')", tx);
      db.planner()
          .executeUpdate("insert into enroll(sid, grade) values(" + i + ", 'grade_" + i + "')", tx);
    }

    var indexes = mdm.getIndexInfo("enroll", tx);
    var sidIdx = indexes.get("sid");

    Plan studentPlan = new TablePlan(tx, "student", mdm);
    Plan enrollPlan = new TablePlan(tx, "enroll", mdm);

    useIndexManually(studentPlan, enrollPlan, sidIdx, "sid");
    useIndexScan(studentPlan, enrollPlan, sidIdx, "sid");

    tx.commit();
  }

  private void useIndexManually(Plan p1, Plan p2, IndexInfo ii, String joinfield) {
    var s1 = p1.open();
    var s2 = (TableScan) p2.open();
    var idx = ii.open();

    var count = 0;
    while (s1.next()) {
      var c = s1.getVal(joinfield);
      idx.beforeFirst(c);
      while (idx.next()) {
        var rid = idx.getDataRid();
        s2.moveToRid(rid);
        assertEquals("grade_" + count, s2.getString("grade"));
        count++;
      }
    }
    assertEquals(count, 2);
    idx.close();
    s1.close();
    s2.close();
  }

  private void useIndexScan(Plan p1, Plan p2, IndexInfo ii, String joinfield) {
    Plan idxPlan = new IndexJoinPlan(p1, p2, ii, joinfield);
    var s = idxPlan.open();

    var count = 0;
    while (s.next()) {
      assertEquals("grade_" + count, s.getString("grade"));
      count++;
    }
    assertEquals(count, 2);
    s.close();
  }
}
