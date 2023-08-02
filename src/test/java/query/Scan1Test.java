package query;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import record.Layout;
import record.Schema;
import record.TableScan;
import server.SimpleDB;

class Scan1Test {
  private SimpleDB db;

  @BeforeEach
  void setup() {
    db = new SimpleDB("test_scan1", 400, 8);
  }

  @AfterEach
  void teardown() {
    db.deleteDB();
  }

  @Test
  void test() {
    var tx = db.newTx();

    var sch1 = new Schema();
    sch1.addIntField("A");
    sch1.addStringField("B", 9);
    var layout = new Layout(sch1);
    UpdateScan s1 = new TableScan(tx, "T", layout);

    s1.beforeFirst();
    var n = 200;
    for (var i = 0; i < n; i++) {
      s1.insert();
      s1.setInt("A", i);
      s1.setString("B", "rec" + i);
    }
    s1.close();

    Scan s2 = new TableScan(tx, "T", layout);

    var t = new Term(new Expression("A"), new Expression(new Constant(10)));
    var pred = new Predicate(t);

    Scan s3 = new SelectScan(s2, pred);
    var fields = List.of("B");
    Scan s4 = new ProjectScan(s3, fields);

    var i = 0;
    while (s4.next()) {
      assertEquals("rec10", s4.getString("B"));
      i++;
    }
    assertEquals(1, i);
    s4.close();
    tx.commit();
  }
}
