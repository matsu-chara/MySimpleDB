package plan;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import server.SimpleDB;

class Planner1Test {
  private SimpleDB db;

  @BeforeEach
  void setup() {
    db = new SimpleDB("test_planner1", 400, 8);
  }

  @AfterEach
  void teardown() {
    db.deleteDB();
  }

  @Test
  void test() {
    var tx = db.newTx();
    var planner = db.planner();

    var cmd1 = "create table T1(A int, B varchar(9))";
    planner.executeUpdate(cmd1, tx);

    var n = 200;
    for (var i = 0; i < n; i++) {
      var valA = i;
      var valB = "'" + "rec" + i + "'";
      var cmd2 = "insert into T1(A,B) values (" + valA + ", " + valB + ")";
      planner.executeUpdate(cmd2, tx);
    }

    var qry = "select B from T1 where A=10";
    var p = planner.createQueryPlan(qry, tx);
    var s = p.open();
    var count = 0;
    while (s.next()) {
      count++;
      assertEquals("rec10", s.getString("b"));
    }
    assertEquals(1, count);

    s.close();
    tx.commit();
  }
}
