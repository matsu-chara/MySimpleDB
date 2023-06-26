package tx;

import buffer.BufferMgr;
import file.BlockId;
import file.FileMgr;
import log.LogMgr;
import tx.recovery.RecoveryMgr;

public class Transaction {
  private FileMgr fm;
  private LogMgr lm;
  private BufferMgr bm;

  private int txnum;
  private static int nextTxNum = 0;
  private RecoveryMgr recoveryMgr;

  public Transaction(FileMgr fm, LogMgr lm, BufferMgr bm) {
    this.fm = fm;
    this.lm = lm;
    this.bm = bm;
    this.txnum = nextTxNumber();
    this.recoveryMgr = new RecoveryMgr(lm, bm, this, txnum);
  }

  public void commit() {
    recoveryMgr.commit();
    System.out.println("Transaction: " + txnum + " committed.");
  }

  public void rollback() {
    recoveryMgr.rollback();
    System.out.println("Transaction: " + txnum + " rollback.");
  }

  public void recover() {
    recoveryMgr.recover();
  }

  public void pin(BlockId blk) {}

  public void unpin(BlockId blk) {}

  public void setInt(BlockId blk, int offset, int val, boolean okToLog) {}

  public void setString(BlockId blk, int offset, String val, boolean okToLog) {}

  private static synchronized int nextTxNumber() {
    nextTxNum++;
    return nextTxNum;
  }
}
