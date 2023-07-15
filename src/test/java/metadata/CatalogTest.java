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
    db.fileMgr().deleteDir();
  }

  @Test
  void test() {
    var tx = db.newTx();

    var tm = new TableMgr(true, tx);

    var tcatLayout = tm.getLayout("tblcat", tx);
    var ts1 = new TableScan(tx, "tblcat", tcatLayout);
    ts1.next();
    assertEquals("tblcat", ts1.getString("tblname"));
    assertEquals(28, ts1.getInt("slotsize"));
    ts1.next();
    assertEquals("fldcat", ts1.getString("tblname"));
    assertEquals(56, ts1.getInt("slotsize"));
    assertFalse(ts1.next());
    ts1.close();

    var fcatLayout = tm.getLayout("fldcat", tx);
    var ts2 = new TableScan(tx, "fldcat", fcatLayout);
    ts2.next();
    assertEquals("tblcat", ts2.getString("tblname"));
    assertEquals("tblname", ts2.getString("fldname"));
    assertEquals(VARCHAR, ts2.getInt("type"));
    assertEquals(16, ts2.getInt("length"));
    assertEquals(4, ts2.getInt("offset"));
    ts2.next();
    assertEquals("tblcat", ts2.getString("tblname"));
    assertEquals("slotsize", ts2.getString("fldname"));
    assertEquals(INTEGER, ts2.getInt("type"));
    assertEquals(0, ts2.getInt("length"));
    assertEquals(24, ts2.getInt("offset"));
    ts2.next();
    assertEquals("fldcat", ts2.getString("tblname"));
    assertEquals("tblname", ts2.getString("fldname"));
    assertEquals(VARCHAR, ts2.getInt("type"));
    assertEquals(16, ts2.getInt("length"));
    assertEquals(4, ts2.getInt("offset"));
    ts2.next();
    assertEquals("fldcat", ts2.getString("tblname"));
    assertEquals("fldname", ts2.getString("fldname"));
    assertEquals(VARCHAR, ts2.getInt("type"));
    assertEquals(16, ts2.getInt("length"));
    assertEquals(24, ts2.getInt("offset"));
    ts2.next();
    assertEquals("fldcat", ts2.getString("tblname"));
    assertEquals("type", ts2.getString("fldname"));
    assertEquals(INTEGER, ts2.getInt("type"));
    assertEquals(0, ts2.getInt("length"));
    assertEquals(44, ts2.getInt("offset"));
    ts2.next();
    assertEquals("fldcat", ts2.getString("tblname"));
    assertEquals("length", ts2.getString("fldname"));
    assertEquals(INTEGER, ts2.getInt("type"));
    assertEquals(0, ts2.getInt("length"));
    assertEquals(48, ts2.getInt("offset"));
    ts2.next();
    assertEquals("fldcat", ts2.getString("tblname"));
    assertEquals("offset", ts2.getString("fldname"));
    assertEquals(INTEGER, ts2.getInt("type"));
    assertEquals(0, ts2.getInt("length"));
    assertEquals(52, ts2.getInt("offset"));

    assertFalse(ts2.next());
    ts2.close();
    tx.commit();
  }
}
