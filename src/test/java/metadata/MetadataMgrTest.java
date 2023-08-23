package metadata;

import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import record.Schema;
import record.TableScan;
import server.SimpleDB;

class MetadataMgrTest {
  private SimpleDB db;

  @BeforeEach
  void setup() {
    db = new SimpleDB("test_metadata_mgr", 400, 8);
  }

  @AfterEach
  void teardown() {
    db.deleteDB();
  }

  @Test
  void test() {
    var tx = db.newTx();
    var mdm = new MetadataMgr(true, tx);

    var sch = new Schema();
    sch.addIntField("A");
    sch.addStringField("B", 9);

    mdm.createTable("MyTable", sch, tx);
    var layout = mdm.getLayout("MyTable", tx);
    var size = layout.slotsize();
    assertEquals(21, size);

    var fld1 = layout.schema().fields().get(0);
    assertEquals(INTEGER, layout.schema().type(fld1));
    var fld2 = layout.schema().fields().get(1);
    assertEquals(VARCHAR, layout.schema().type(fld2));

    var ts = new TableScan(tx, "MyTable", layout);
    for (var i = 0; i < 50; i++) {
      ts.insert();
      ts.setInt("A", i);
      ts.setString("B", "rec" + i);
    }
    var si = mdm.getStatInfo("MyTable", layout, tx);
    assertEquals(3, si.blocksAccessed());
    assertEquals(50, si.recordsOutput());
    assertEquals(16, si.distinctValues());

    var viewdef = "select B from MyTable where A = 1;";
    mdm.createView("viewA", viewdef, tx);
    var v = mdm.getViewDef("viewA", tx);
    assertEquals(viewdef, v);

    mdm.createIndex("indexA", "MyTable", "A", tx);
    mdm.createIndex("indexB", "MyTable", "B", tx);

    var idxmap = mdm.getIndexInfo("MyTable", tx);

    var iiA = idxmap.get("A");
    assertEquals(0, iiA.blocksAccessed());
    assertEquals(3, iiA.recordsOutput());
    assertEquals(1, iiA.distinctValues("A"));
    assertEquals(16, iiA.distinctValues("B"));

    var iiB = idxmap.get("B");
    assertEquals(0, iiB.blocksAccessed());
    assertEquals(3, iiB.recordsOutput());
    assertEquals(16, iiB.distinctValues("A"));
    assertEquals(1, iiB.distinctValues("B"));
    tx.commit();
  }
}
