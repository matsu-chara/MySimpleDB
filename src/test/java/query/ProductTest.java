package query;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import record.Layout;
import record.Schema;
import record.TableScan;
import server.SimpleDB;

class ProductTest {
  private SimpleDB db;

  @BeforeEach
  void setup() {
    db = new SimpleDB("test_product", 400, 8);
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
    var layout1 = new Layout(sch1);
    var ts1 = new TableScan(tx, "T1", layout1);

    var sch2 = new Schema();
    sch2.addIntField("C");
    sch2.addStringField("D", 9);
    var layout2 = new Layout(sch2);
    var ts2 = new TableScan(tx, "T2", layout2);

    ts1.beforeFirst();
    var n = 200;
    for (var i = 0; i < n; i++) {
      ts1.insert();
      ts1.setInt("A", i);
      ts1.setString("B", "rec" + i);
    }
    ts1.close();

    ts2.beforeFirst();
    for (var i = 0; i < n; i++) {
      ts2.insert();
      ts2.setInt("C", n - i - 1);
      ts2.setString("D", "rec" + (n - i - 1));
    }
    ts2.close();

    Scan s1 = new TableScan(tx, "T1", layout1);
    Scan s2 = new TableScan(tx, "T2", layout2);
    Scan s3 = new ProductScan(s1, s2);
    var count = 0;
    while (s3.next()) {
      count++;
    }
    assertEquals(200 * 200, count);
    s3.close();
    tx.commit();
  }
}
