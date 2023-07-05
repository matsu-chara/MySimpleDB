package tx;

import buffer.BufferMgr;
import file.BlockId;
import file.FileMgr;
import log.LogMgr;
import tx.concurrency.ConcurrencyMgr;
import tx.recovery.RecoveryMgr;

public class Transaction {
  private FileMgr fm;
  private BufferMgr bm;

  private int txnum;
  private static int nextTxNum = 0;
  private static int END_OF_FILE = -1;

  private RecoveryMgr recoveryMgr;
  private ConcurrencyMgr concurMgr;

  private BufferList mybuffers;

  public Transaction(FileMgr fm, LogMgr lm, BufferMgr bm) {
    this.fm = fm;
    this.bm = bm;
    this.txnum = nextTxNumber();
    this.recoveryMgr = new RecoveryMgr(lm, bm, this, txnum);
    this.concurMgr = new ConcurrencyMgr();
    this.mybuffers = new BufferList(bm);
    System.out.println("Transaction: " + txnum + " started."); // log目的で残しておく
  }

  public void commit() {
    recoveryMgr.commit();
    concurMgr.release();
    mybuffers.unpinAll();
    System.out.println("Transaction: " + txnum + " committed."); // log目的で残しておく
  }

  public void rollback() {
    recoveryMgr.rollback();
    concurMgr.release();
    mybuffers.unpinAll();
    System.out.println("Transaction: " + txnum + " rollbacked."); // log目的で残しておく
  }

  public void recover() {
    bm.flushAll(
        txnum); // recoveryMgrもflushするので不要といえば不要。(bufferPool全体をflushしてるけどmybuffersをflushするようにしたらちょっとだけ性能上がりそう）
    recoveryMgr.recover();
  }

  public void pin(BlockId blk) {
    mybuffers.pin(blk);
  }

  public void unpin(BlockId blk) {
    mybuffers.unpin(blk);
  }

  public int getInt(BlockId blk, int offset) {
    concurMgr.sLock(blk);
    var buff = mybuffers.getBuffer(blk);
    return buff.contents().getInt(offset);
  }

  public String getString(BlockId blk, int offset) {
    concurMgr.sLock(blk);
    var buff = mybuffers.getBuffer(blk);
    return buff.contents().getString(offset);
  }

  public void setInt(BlockId blk, int offset, int val, boolean okToLog) {
    concurMgr.xLock(blk);
    var buff = mybuffers.getBuffer(blk);
    var lsn = -1;
    if (okToLog) {
      lsn = recoveryMgr.setInt(buff, offset);
    }
    buff.contents().setInt(offset, val);
    buff.setModified(txnum, lsn);
  }

  public void setString(BlockId blk, int offset, String val, boolean okToLog) {
    concurMgr.xLock(blk);
    var buff = mybuffers.getBuffer(blk);
    var lsn = -1;
    if (okToLog) {
      lsn = recoveryMgr.setString(buff, offset);
    }
    buff.contents().setString(offset, val);
    buff.setModified(txnum, lsn);
  }

  public int size(String filename) {
    var dummyblk = new BlockId(filename, END_OF_FILE);
    concurMgr.sLock(dummyblk);
    return fm.length(filename);
  }

  public BlockId append(String filename) {
    var dummyblk = new BlockId(filename, END_OF_FILE);
    concurMgr.sLock(dummyblk);
    return fm.append(filename);
  }

  public int blockSize() {
    return fm.blockSize();
  }

  public int availableBuffers() {
    return bm.available();
  }

  private static synchronized int nextTxNumber() {
    nextTxNum++;
    return nextTxNum;
  }

  // visible for test
  public int txnum() {
    return txnum;
  }
}
