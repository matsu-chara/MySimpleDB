package query;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import record.Layout;
import record.Schema;
import record.TableScan;
import server.SimpleDB;

class ScanTest2 {
  private SimpleDB db;

  @BeforeEach
  void setup() {
    db = new SimpleDB("test_scan2", 400, 8);
  }

  @AfterEach
  void teardown() {
    db.fileMgr().deleteDir();
  }

  @Test
  void test() {
    var tx = db.newTx();

    var sch1 = new Schema();
    sch1.addIntField("A");
    sch1.addStringField("B", 9);
    var layout1 = new Layout(sch1);
    UpdateScan us1 = new TableScan(tx, "T1", layout1);

    us1.beforeFirst();
    var n = 200;
    for (var i = 0; i < n; i++) {
      us1.insert();
      us1.setInt("A", i);
      us1.setString("B", "bbb" + i);
    }
    us1.close();

    var sch2 = new Schema();
    sch2.addIntField("C");
    sch2.addStringField("D", 9);
    var layout2 = new Layout(sch2);
    var us2 = new TableScan(tx, "T2", layout2);

    us2.beforeFirst();
    for (var i = 0; i < n; i++) {
      us2.insert();
      us2.setInt("C", n - i - 1);
      us2.setString("D", "ddd" + (n - i - 1));
    }
    us2.close();

    Scan s1 = new TableScan(tx, "T1", layout1);
    Scan s2 = new TableScan(tx, "T2", layout2);
    Scan s3 = new ProductScan(s1, s2);

    var t = new Term(new Expression("A"), new Expression("C"));
    var pred = new Predicate(t);
    Scan s4 = new SelectScan(s3, pred);

    var c = Arrays.asList("B", "D");
    Scan s5 = new ProjectScan(s4, c);
    var count = 0;

    s5.next();
    count++;
    assertEquals("bbb0", s5.getString("B"));
    assertEquals("ddd0", s5.getString("D"));
    s5.next();
    count++;
    assertEquals("bbb1", s5.getString("B"));
    assertEquals("ddd1", s5.getString("D"));

    while (s5.next()) {
      count++;
    }
    assertEquals(200, count);

    s5.close();
    tx.commit();
  }
}
