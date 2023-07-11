package record;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import server.SimpleDB;

class TableScanTest {
  private SimpleDB db;

  @BeforeEach
  void setup() {
    db = new SimpleDB("test_table_scan", 400, 8);
  }

  @AfterEach
  void teardown() {
    db.fileMgr().deleteDir();
  }

  @Test
  void test() {
    var tx = db.newTx();
    var sch = new Schema();

    sch.addIntField("A");
    sch.addStringField("B", 9);
    var layout = new Layout(sch);

    var ts = new TableScan(tx, "T", layout);
    for (var i = 0; i < 20; i++) {
      ts.insert();
      ts.setInt("A", i);
      ts.setString("B", "rec" + i);
      assertEquals(new RID(i / 19, i % 19), ts.getRid());
    }

    var i = 0;
    ts.beforeFirst();
    while (ts.next()) {
      assertEquals("rec" + i, ts.getString("B"));
      if (i < 25) {
        ts.delete();
      }
      i++;
    }

    ts.beforeFirst();
    while (ts.next()) {
      assertTrue(ts.getInt("A") >= 25);
    }

    ts.close();
    tx.commit();
  }
}
