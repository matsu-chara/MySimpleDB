package tx.recovery;

import static org.junit.jupiter.api.Assertions.*;

import buffer.BufferMgr;
import file.BlockId;
import file.FileMgr;
import file.Page;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import server.SimpleDB;
import tx.Transaction;

class RecoveryTest {
  public static FileMgr fm;
  public static BufferMgr bm;
  public static SimpleDB db;
  private static BlockId blk0, blk1;

  @BeforeEach
  void setup() {
    db = new SimpleDB("test_recovery", 400, 8);
    fm = db.fileMgr();
    bm = db.bufferMgr();
    blk0 = new BlockId("testfile", 0);
    blk1 = new BlockId("testfile", 1);
  }

  @AfterEach
  void teardown() {
    fm.deleteFile("testfile");
    fm.deleteFile("simpledb.log");
  }

  @Test
  void test() {
    initialize();

    var tx3 = db.newTx();
    tx3.pin(blk0);
    @SuppressWarnings("PointlessArithmeticExpression")
    var expected1 =
        new TestRecord(
            Arrays.asList(
                Integer.BYTES * 0,
                Integer.BYTES * 1,
                Integer.BYTES * 2,
                Integer.BYTES * 3,
                Integer.BYTES * 4,
                Integer.BYTES * 5),
            List.of("abc"));
    @SuppressWarnings("PointlessArithmeticExpression")
    var actual1 =
        new TestRecord(
            Arrays.asList(
                tx3.getInt(blk0, Integer.BYTES * 0),
                tx3.getInt(blk0, Integer.BYTES * 1),
                tx3.getInt(blk0, Integer.BYTES * 2),
                tx3.getInt(blk0, Integer.BYTES * 3),
                tx3.getInt(blk0, Integer.BYTES * 4),
                tx3.getInt(blk0, Integer.BYTES * 5)),
            List.of(tx3.getString(blk0, 30)));
    assertEquals(expected1, actual1);

    tx3.pin(blk1);
    @SuppressWarnings("PointlessArithmeticExpression")
    var expected2 =
        new TestRecord(
            Arrays.asList(
                Integer.BYTES * 0,
                Integer.BYTES * 1,
                Integer.BYTES * 2,
                Integer.BYTES * 3,
                Integer.BYTES * 4,
                Integer.BYTES * 5),
            List.of("def"));
    @SuppressWarnings("PointlessArithmeticExpression")
    var actual2 =
        new TestRecord(
            Arrays.asList(
                tx3.getInt(blk1, Integer.BYTES * 0),
                tx3.getInt(blk1, Integer.BYTES * 1),
                tx3.getInt(blk1, Integer.BYTES * 2),
                tx3.getInt(blk1, Integer.BYTES * 3),
                tx3.getInt(blk1, Integer.BYTES * 4),
                tx3.getInt(blk1, Integer.BYTES * 5)),
            List.of(tx3.getString(blk1, 30)));
    assertEquals(expected2, actual2);
    tx3.commit();

    var tx5 = modify();

    tx3.pin(blk0);
    var expected3 = expected1; // tx4はrollback済みなので変更されない
    @SuppressWarnings("PointlessArithmeticExpression")
    var actual3 =
        new TestRecord(
            Arrays.asList(
                tx3.getInt(blk0, Integer.BYTES * 0),
                tx3.getInt(blk0, Integer.BYTES * 1),
                tx3.getInt(blk0, Integer.BYTES * 2),
                tx3.getInt(blk0, Integer.BYTES * 3),
                tx3.getInt(blk0, Integer.BYTES * 4),
                tx3.getInt(blk0, Integer.BYTES * 5)),
            List.of(tx3.getString(blk0, 30)));
    assertEquals(expected3, actual3);

    var p = new Page(fm.blockSize());
    fm.read(blk1, p);
    @SuppressWarnings("PointlessArithmeticExpression")
    var expected4 =
        new TestRecord( // tx5は未コミットだがflushはされてるので読める
            Arrays.asList(
                Integer.BYTES * 0 + 100,
                Integer.BYTES * 1 + 100,
                Integer.BYTES * 2 + 100,
                Integer.BYTES * 3 + 100,
                Integer.BYTES * 4 + 100,
                Integer.BYTES * 5 + 100),
            List.of("xyz"));
    @SuppressWarnings("PointlessArithmeticExpression")
    var actual4 =
        new TestRecord(
            Arrays.asList(
                p.getInt(Integer.BYTES * 0),
                p.getInt(Integer.BYTES * 1),
                p.getInt(Integer.BYTES * 2),
                p.getInt(Integer.BYTES * 3),
                p.getInt(Integer.BYTES * 4),
                p.getInt(Integer.BYTES * 5)),
            List.of(p.getString(30)));
    assertEquals(expected4, actual4);

    tx5.commit();
    tx3.commit();
  }

  private void initialize() {
    var tx1 = db.newTx();
    var tx2 = db.newTx();

    tx1.pin(blk0);
    tx2.pin(blk1);

    var pos = 0;
    for (var i = 0; i < 6; i++) {
      tx1.setInt(blk0, pos, pos, false);
      tx2.setInt(blk1, pos, pos, false);
      pos += Integer.BYTES;
    }
    tx1.setString(blk0, 30, "abc", false);
    tx2.setString(blk1, 30, "def", false);
    tx1.commit();
    tx2.commit();
  }

  private Transaction modify() {
    var tx4 = db.newTx();
    var tx5 = db.newTx();

    tx4.pin(blk0);
    tx5.pin(blk1);
    var pos = 0;
    for (var i = 0; i < 6; i++) {
      tx4.setInt(blk0, pos, pos + 100, true);
      tx5.setInt(blk1, pos, pos + 100, true);
      pos += Integer.BYTES;
    }
    tx4.setString(blk0, 30, "utw", true);
    tx5.setString(blk1, 30, "xyz", true);

    tx4.rollback();
    bm.flushAll(tx5.txnum()); // tx5の内容が見えるようにflushしておく

    return tx5; // 他のテストに影響が出るのでtx5を後でcommitするために返却する
  }

  private static class TestRecord {
    private List<Integer> is;
    private List<String> ss;

    @Override
    public String toString() {
      return "TestRecord{" + "is=" + is + ", ss=" + ss + '}';
    }

    public TestRecord(List<Integer> is, List<String> ss) {
      this.is = is;
      this.ss = ss;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      var that = (TestRecord) o;

      if (!Objects.equals(is, that.is)) return false;
      return Objects.equals(ss, that.ss);
    }

    @Override
    public int hashCode() {
      var result = is != null ? is.hashCode() : 0;
      result = 31 * result + (ss != null ? ss.hashCode() : 0);
      return result;
    }
  }
}
