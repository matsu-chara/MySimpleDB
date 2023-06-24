package record;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import server.SimpleDB;

class RecordPageTest {
  private SimpleDB db;

  @BeforeEach
  void setup() {
    db = new SimpleDB("test_record_page", 400, 8);
  }

  @AfterEach
  void teardown() {
    db.fm().deleteFile("testfile");
    db.fm().deleteFile("simpledb.log");
  }

  @Test
  void test() {
    var tx = db.newTx();
    var sch = new Schema();
    sch.addIntField("A");
    sch.addStringField("B", 9);
    var layout = new Layout(sch);

    assertEquals(Integer.BYTES, layout.offsets("A"));
    assertEquals(Integer.BYTES * 2, layout.offsets("B"));

    var blk = tx.append("testfile");
    tx.pin(blk);
    var rp = new RecordPage(tx, blk, layout);
    rp.format();

    var slot = rp.insertAfter(-1);
    while (slot >= 0) {
      rp.setInt(slot, "A", slot);
      rp.setString(slot, "B", "rec" + slot);
      slot = rp.insertAfter(slot);
    }

    slot = rp.nextAfter(-1);
    while (slot >= 0) {
      var a = rp.getInt(slot, "A");
      var b = rp.getString(slot, "B");
      assertEquals(slot, a);
      assertEquals("rec" + slot, b);
      rp.delete(slot);
      slot = rp.nextAfter(slot);
    }

    slot = rp.nextAfter(-1);
    while (slot >= 0) {
      var a = rp.getInt(slot, "A");
      var b = rp.getString(slot, "B");
      assertEquals(slot, a);
      assertEquals("rec" + slot, b);
      slot = rp.nextAfter(slot);
    }
    tx.unpin(blk);
    tx.commit();
  }
}
