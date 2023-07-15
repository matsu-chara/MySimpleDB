package metadata;

import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import record.Schema;
import server.SimpleDB;

class TableMgrTest {
  private SimpleDB db;

  @BeforeEach
  void setup() {
    db = new SimpleDB("test_table_mgr", 400, 8);
  }

  @AfterEach
  void teardown() {
    db.fileMgr().deleteDir();
  }

  @Test
  void test() {
    var tx = db.newTx();

    var tm = new TableMgr(true, tx);
    var sch = new Schema();
    sch.addIntField("A");
    sch.addStringField("B", 9);
    tm.createTable("MyTable", sch, tx);

    var layout = tm.getLayout("MyTable", tx);
    var size = layout.slotsize();
    assertEquals(21, size);

    var sch2 = layout.schema();
    assertEquals(INTEGER, sch2.type("A"));
    assertEquals(0, sch2.length("A"));
    assertEquals(4, layout.offsets("A"));
    assertEquals(VARCHAR, sch2.type("B"));
    assertEquals(9, sch2.length("B"));
    assertEquals(8, layout.offsets("B"));

    tx.commit();
  }
}
