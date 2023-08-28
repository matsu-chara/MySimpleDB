package index.planner;

import static org.junit.jupiter.api.Assertions.assertNotEquals;

import index.Index;
import java.util.HashMap;
import metadata.IndexInfo;
import metadata.MetadataMgr;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import plan.TablePlan;
import query.UpdateScan;
import record.Schema;
import server.SimpleDB;

public class BtreeIndexUpdateTest {
  private SimpleDB db;
  private MetadataMgr mdm;

  @BeforeEach
  void setup() {
    db = new SimpleDB("test_index_join_update_planner_hash", 400, 8, IndexInfo.IndexMode.BTree);
    mdm = db.mdMgr();

    var tx = db.newTx();

    var sch1 = new Schema();
    sch1.addIntField("sid");
    sch1.addIntField("majorid");
    sch1.addIntField("gradeyear");
    sch1.addStringField("sname", 6);
    mdm.createTable("student", sch1, tx);
    mdm.createIndex("majorid", "student", "majorid", tx);
    mdm.createIndex("gradeyear", "student", "gradeyear", tx);

    tx.commit();
  }

  @AfterEach
  void teardown() {
    db.deleteDB();
  }

  @Test
  void test() {
    var tx = db.newTx();
    var studentPlan = new TablePlan(tx, "student", mdm);
    var studentScan = (UpdateScan) studentPlan.open();

    var indexes = new HashMap<String, Index>();
    var idxinfo = mdm.getIndexInfo("student", tx);
    for (var fldname : idxinfo.keySet()) {
      var idx = idxinfo.get(fldname).open();
      indexes.put(fldname, idx);
    }

    studentScan.insert();
    studentScan.setInt("sid", 11);
    studentScan.setString("sname", "sam");
    studentScan.setInt("gradeyear", 2023);
    studentScan.setInt("majorid", 30);
    studentScan.insert();
    studentScan.setInt("sid", 12);
    studentScan.setString("sname", "joe");
    studentScan.setInt("gradeyear", 2023);
    studentScan.setInt("majorid", 30);

    var datarid = studentScan.getRid();
    for (var fldname : indexes.keySet()) {
      var dataval = studentScan.getVal(fldname);
      var idx = indexes.get(fldname);
      idx.insert(dataval, datarid);
    }

    studentScan.beforeFirst();
    while (studentScan.next()) {
      if (studentScan.getString("sname").equals("joe")) {
        var joeRID = studentScan.getRid();
        for (var fldname : indexes.keySet()) {
          var dataval = studentScan.getVal(fldname);
          var idx = indexes.get(fldname);
          idx.delete(dataval, joeRID);
        }
        studentScan.delete();
        break;
      }
    }

    studentScan.beforeFirst();
    while (studentScan.next()) {
      assertNotEquals("joe", studentScan.getString("sname"));
    }
    studentScan.close();
    for (var idx : indexes.values()) {
      idx.close();
    }

    tx.commit();
  }
}
