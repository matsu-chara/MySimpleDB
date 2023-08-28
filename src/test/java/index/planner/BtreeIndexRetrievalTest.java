package index.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;

import metadata.IndexInfo;
import metadata.MetadataMgr;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import plan.TablePlan;
import query.Constant;
import query.UpdateScan;
import record.Schema;
import server.SimpleDB;

public class BtreeIndexRetrievalTest {
  private SimpleDB db;
  private MetadataMgr mdm;

  @BeforeEach
  void setup() {
    db = new SimpleDB("test_index_join_planner_btree", 400, 8, IndexInfo.IndexMode.BTree);
    mdm = db.mdMgr();

    var tx = db.newTx();

    var sch1 = new Schema();
    sch1.addIntField("sid");
    sch1.addIntField("majorid");
    sch1.addStringField("sname", 6);
    mdm.createTable("student", sch1, tx);
    mdm.createIndex("majorid", "student", "majorid", tx);

    tx.commit();
  }

  @AfterEach
  void teardown() {
    db.deleteDB();
  }

  @Test
  void test() {
    var tx = db.newTx();
    for (var i = 15; i < 25; i++) {
      db.planner()
          .executeUpdate(
              "insert into student(sid, majorid, sname) values("
                  + i
                  + ", "
                  + i
                  + ", 'sname_"
                  + i
                  + "')",
              tx);
    }

    var studentPlan = new TablePlan(tx, "student", mdm);
    var studentScan = (UpdateScan) studentPlan.open();

    var indexes = mdm.getIndexInfo("student", tx);
    var ii = indexes.get("majorid");
    var idx = ii.open();

    var count = 0;
    idx.beforeFirst(new Constant(20));
    while (idx.next()) {
      var rid = idx.getDataRid();
      studentScan.moveToRid(rid);
      assertEquals("sname_20", studentScan.getString("sname"));
      count++;
    }
    assertEquals(1, count);

    tx.commit();
  }
}
