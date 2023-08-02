package metadata;

import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import record.TableScan;
import server.SimpleDB;

class CatalogTest {
  private SimpleDB db;

  @BeforeEach
  void setup() {
    db = new SimpleDB("test_catalog", 400, 8);
  }

  @AfterEach
  void teardown() {
    db.deleteDB();
  }

  @Test
  void test() {
    var tx = db.newTx();

    var tm = new TableMgr(false, tx);

    var tcatLayout = tm.getLayout("tblcat", tx);
    var ts1 = new TableScan(tx, "tblcat", tcatLayout);

    // ここではtblcat, fldcatテーブルについて検証する。(実際にはviewやindexなどのテーブルも作成されている
    var count = 0;
    while (ts1.next()) {
      var tblname = ts1.getString("tblname");
      if (tblname.equals("tblcat")) {
        assertEquals(28, ts1.getInt("slotsize"));
        count++;
      } else if (tblname.equals("fldcat")) {
        assertEquals(56, ts1.getInt("slotsize"));
        count++;
      }
    }
    assertEquals(2, count);
    ts1.close();

    var fcatLayout = tm.getLayout("fldcat", tx);
    var ts2 = new TableScan(tx, "fldcat", fcatLayout);
    var count2 = 0;
    while (ts2.next()) {
      var tblname = ts2.getString("tblname");
      var fldname = ts2.getString("fldname");
      if (tblname.equals("tblcat") && fldname.equals("tblname")) {
        count2++;
        assertEquals(VARCHAR, ts2.getInt("type"));
        assertEquals(16, ts2.getInt("length"));
        assertEquals(4, ts2.getInt("offset"));
      } else if (tblname.equals("tblcat") && fldname.equals("slotsize")) {
        count2++;
        assertEquals(INTEGER, ts2.getInt("type"));
        assertEquals(0, ts2.getInt("length"));
        assertEquals(24, ts2.getInt("offset"));
      } else if (tblname.equals("tblcat") && fldname.equals("fldcat")) {
        count2++;
        assertEquals(VARCHAR, ts2.getInt("type"));
        assertEquals(16, ts2.getInt("length"));
        assertEquals(4, ts2.getInt("offset"));
      } else if (tblname.equals("fldcat") && fldname.equals("fldname")) {
        count2++;
        assertEquals(VARCHAR, ts2.getInt("type"));
        assertEquals(16, ts2.getInt("length"));
        assertEquals(24, ts2.getInt("offset"));
      } else if (tblname.equals("fldcat") && fldname.equals("type")) {
        count2++;
        assertEquals(INTEGER, ts2.getInt("type"));
        assertEquals(0, ts2.getInt("length"));
        assertEquals(44, ts2.getInt("offset"));
      } else if (tblname.equals("fldcat") && fldname.equals("length")) {
        count2++;
        assertEquals(INTEGER, ts2.getInt("type"));
        assertEquals(0, ts2.getInt("length"));
        assertEquals(48, ts2.getInt("offset"));
      } else if (tblname.equals("fldcat") && fldname.equals("offset")) {
        count2++;
        assertEquals(INTEGER, ts2.getInt("type"));
        assertEquals(0, ts2.getInt("length"));
        assertEquals(52, ts2.getInt("offset"));
      }
    }
    assertEquals(6, count2);

    ts2.close();
    tx.commit();
  }
}
