package plan;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import server.SimpleDB;

class Planner2Test {
  private SimpleDB db;

  @BeforeEach
  void setup() {
    db = new SimpleDB("test_planner2", 400, 8);
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
    var n1 = 200;
    for (var i = 0; i < n1; i++) {
      var valA = i;
      var valB = "'" + "bbb" + i + "'";
      var cmd2 = "insert into T1(A,B) values (" + valA + ", " + valB + ")";
      planner.executeUpdate(cmd2, tx);
    }

    var cmd3 = "create table T2(C int, D varchar(9))";
    planner.executeUpdate(cmd3, tx);
    var n2 = 200;
    for (var i = 0; i < n2; i++) {
      var valC = n2 - i - 1;
      var valD = "'" + "ddd" + valC + "'";
      var cmd4 = "insert into T2(C,D) values (" + valC + ", " + valD + ")";
      planner.executeUpdate(cmd4, tx);
    }

    var qry = "select B,D from T1,T2 where A=C";
    var p = planner.createQueryPlan(qry, tx);
    var s = p.open();
    var count = 0;

    s.next();
    assertEquals("bbb0", s.getString("b"));
    assertEquals("ddd0", s.getString("d"));
    count++;

    while (s.next()) {
      count++;
    }
    assertEquals(200, count);

    s.close();
    tx.commit();
  }
}
