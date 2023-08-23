package index.query;

import static org.junit.jupiter.api.Assertions.*;

import index.planner.IndexSelectPlan;
import metadata.IndexInfo;
import metadata.MetadataMgr;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import plan.Plan;
import plan.TablePlan;
import query.Constant;
import record.Schema;
import record.TableScan;
import server.SimpleDB;

class IndexSelectTest {
  private SimpleDB db;
  private MetadataMgr mdm;

  @BeforeEach
  void setup() {
    db = new SimpleDB("test_scan1", 400, 8);
    mdm = db.mdMgr();

    var tx = db.newTx();
    var sch = new Schema();
    sch.addIntField("sid");
    sch.addStringField("grade", 6);
    mdm.createTable("enroll", sch, tx);
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
    for (var i = 0; i < 10; i++) {
      db.planner()
          .executeUpdate("insert into enroll(sid, grade) values(" + i + ", 'grade_" + i + "')", tx);
    }

    var indexes = mdm.getIndexInfo("enroll", tx);
    var sidIdx = indexes.get("sid");

    Plan enrollPlan = new TablePlan(tx, "enroll", mdm);

    var c = new Constant(6);

    useIndexManually(sidIdx, enrollPlan, c);
    useIndexScan(sidIdx, enrollPlan, c);

    tx.commit();
  }

  private void useIndexManually(IndexInfo ii, Plan p, Constant c) {
    var s = (TableScan) p.open();
    var idx = ii.open();

    var count = 0;

    idx.beforeFirst(c);
    while (idx.next()) {
      var rid = idx.getDataRid();
      s.moveToRid(rid);
      assertEquals("grade_6", s.getString("grade"));
      count++;
    }
    assertEquals(1, count);
    idx.close();
    s.close();
  }

  private void useIndexScan(IndexInfo ii, Plan p, Constant c) {
    Plan idxPlan = new IndexSelectPlan(p, ii, c);
    var s = idxPlan.open();

    var count = 0;

    while (s.next()) {
      assertEquals("grade_6", s.getString("grade"));
      count++;
    }
    assertEquals(1, count);
    s.close();
  }
}
