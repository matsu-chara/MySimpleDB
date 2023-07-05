package tx;

import static org.junit.jupiter.api.Assertions.*;

import buffer.BufferMgr;
import file.BlockId;
import file.FileMgr;
import log.LogMgr;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import server.SimpleDB;

class TransactionTest {
  public static FileMgr fm;
  private static LogMgr lm;
  public static BufferMgr bm;
  public static SimpleDB db;

  @BeforeEach
  void setup() {
    db = new SimpleDB("test_transaction", 400, 8);
    fm = db.fm();
    bm = db.bm();
    lm = db.lm();
  }

  @AfterEach
  void teardown() {
    fm.deleteFile("testfile");
    fm.deleteFile("simpledb.log");
  }

  @Test
  void test() {
    var blk = new BlockId("testfile", 1);

    // ブロックの初期状態は（本来は）不明な値が入っているだけ
    // なのでロールバックしてほしくない => ログにはかかない
    var tx1 = new Transaction(fm, lm, bm);
    tx1.pin(blk);
    tx1.setInt(blk, 80, 1, false);
    tx1.setString(blk, 40, "one", false);
    tx1.commit();

    var tx2 = new Transaction(fm, lm, bm);
    tx2.pin(blk);
    var ival = tx2.getInt(blk, 80);
    var sval = tx2.getString(blk, 40);
    assertEquals(1, ival);
    assertEquals("one", sval);

    var newival = ival + 1;
    var newsval = sval + "!";

    tx2.setInt(blk, 80, newival, true);
    tx2.setString(blk, 40, newsval, true);
    tx2.commit();

    var tx3 = new Transaction(fm, lm, bm);
    tx3.pin(blk);
    assertEquals(2, tx3.getInt(blk, 80));
    assertEquals("one!", tx3.getString(blk, 40));

    tx3.setInt(blk, 80, 9999, true);
    assertEquals(9999, tx3.getInt(blk, 80));
    tx3.rollback();

    var tx4 = new Transaction(fm, lm, bm);
    tx4.pin(blk);
    assertEquals(2, tx4.getInt(blk, 80));
    tx4.commit();
  }
}
