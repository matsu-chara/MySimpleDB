package tx.concurrency;

import static org.junit.jupiter.api.Assertions.*;

import buffer.BufferMgr;
import file.BlockId;
import file.FileMgr;
import log.LogMgr;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import server.SimpleDB;
import tx.Transaction;

class ConcurrencyTest {
  public static FileMgr fm;
  private static LogMgr lm;
  public static BufferMgr bm;
  public static SimpleDB db;

  @BeforeEach
  void setup() {
    db = new SimpleDB("test_concurrency", 400, 8);
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
    Thread a = new Thread(new A());
    Thread b = new Thread(new B());
    Thread c = new Thread(new C());

    a.start();
    b.start();
    c.start();
    try {
      a.join();
      b.join();
      c.join();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    Transaction tx = db.newTx();
    BlockId blk1 = new BlockId("testfile", 1);
    BlockId blk2 = new BlockId("testfile", 1);
    tx.pin(blk1);
    tx.pin(blk2);
    int actual1 = tx.getInt(blk1, 0);
    int actual2 = tx.getInt(blk2, 0);

    assertEquals(0, actual1);
    assertEquals(0, actual2);
    tx.commit();
  }

  static class A implements Runnable {
    @Override
    public void run() {
      try {
        Transaction txA = new Transaction(fm, lm, bm);
        BlockId blk1 = new BlockId("testfile", 1);
        BlockId blk2 = new BlockId("testfile", 2);
        txA.pin(blk1);
        txA.pin(blk2);
        System.out.println("Tx A: request slock 1");
        txA.getInt(blk1, 0);
        System.out.println("Tx A: receive slock 1");
        Thread.sleep(100);
        System.out.println("Tx A: request slock 2");
        txA.getInt(blk2, 0);
        System.out.println("Tx A: receive slock 2");
        txA.commit();
        System.out.println("Tx A: commit");
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static class B implements Runnable {
    @Override
    public void run() {
      try {
        Transaction txB = new Transaction(fm, lm, bm);
        BlockId blk1 = new BlockId("testfile", 1);
        BlockId blk2 = new BlockId("testfile", 2);
        txB.pin(blk1);
        txB.pin(blk2);
        System.out.println("Tx B: request xlock 2");
        txB.setInt(blk2, 0, 0, false);
        System.out.println("Tx B: receive xlock 2");
        Thread.sleep(100);
        System.out.println("Tx B: request slock 1");
        txB.getInt(blk1, 0);
        System.out.println("Tx B: receive slock 1");
        txB.commit();
        System.out.println("Tx B commit");
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static class C implements Runnable {
    @Override
    public void run() {
      try {
        Transaction txC = new Transaction(fm, lm, bm);
        BlockId blk1 = new BlockId("testfile", 1);
        BlockId blk2 = new BlockId("testfile", 2);
        txC.pin(blk1);
        txC.pin(blk2);
        Thread.sleep(50);
        System.out.println("Tx C: request xlock 1");
        txC.setInt(blk1, 0, 0, false);
        System.out.println("Tx C: receive xlock 1");
        Thread.sleep(100);
        System.out.println("Tx C: request slock 2");
        txC.getInt(blk2, 0);
        System.out.println("Tx C: receive slock 2");
        txC.commit();
        System.out.println("Tx C commit");
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
