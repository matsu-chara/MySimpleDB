package tx.recovery;

import static tx.recovery.LogRecord.CHECKPOINT;
import static tx.recovery.LogRecord.COMMIT;
import static tx.recovery.LogRecord.ROLLBACK;
import static tx.recovery.LogRecord.START;

import buffer.Buffer;
import buffer.BufferMgr;
import file.BlockId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import log.LogMgr;
import tx.Transaction;

public class RecoveryMgr {
  private LogMgr lm;
  private BufferMgr bm;
  private Transaction tx;
  private int txnum;

  public RecoveryMgr(LogMgr lm, BufferMgr bm, Transaction tx, int txnum) {
    this.lm = lm;
    this.bm = bm;
    this.tx = tx;
    this.txnum = txnum;
    StartRecord.writeToLog(lm, txnum);
  }

  public void commit() {
    bm.flushAll(txnum); // log書き込み => pageをblockにflush => commitRecordをログにflush
    int lsn = CommitRecord.writeToLog(lm, txnum);
    lm.flush(lsn);
  }

  public void rollback() {
    doRollBack();
    bm.flushAll(txnum);
    int lsn = RollbackRecord.writeToLog(lm, txnum);
    lm.flush(lsn);
  }

  public void recover() {
    doRecover();
    bm.flushAll(txnum);
    int lsn = CheckpointRecord.writeToLog(lm);
    lm.flush(lsn);
  }

  public int setInt(Buffer buff, int offset) {
    int oldval = buff.contents().getInt(offset);
    BlockId blk = buff.block();
    return SetIntRecord.writeToLog(lm, txnum, blk, offset, oldval);
  }

  public int setString(Buffer buff, int offset) {
    String oldval = buff.contents().getString(offset);
    BlockId blk = buff.block();
    return SetStringRecord.writeToLog(lm, txnum, blk, offset, oldval);
  }

  private void doRollBack() {
    Iterator<byte[]> iter = lm.iterator();
    while (iter.hasNext()) {
      byte[] bytes = iter.next();
      LogRecord rec = LogRecord.createLogRecord(bytes);
      if (rec.txNumber() == txnum) { // いま考えてるtxのレコードのみをrollbackする
        if (rec.op() == START) { // beginを見つけたら終わり
          return;
        }
        rec.undo(tx);
      }
    }
  }

  private void doRecover() {
    Collection<Integer> finishedTxs = new ArrayList<>();
    Iterator<byte[]> iter = lm.iterator();
    while (iter.hasNext()) {
      byte[] bytes = iter.next();
      LogRecord rec = LogRecord.createLogRecord(bytes);
      if (rec.op() == CHECKPOINT) {
        return; // CHECKPOINTレコードかlogの終端まで読み切ったらおわり
      } else if (rec.op() == COMMIT || rec.op() == ROLLBACK) {
        finishedTxs.add(rec.txNumber()); // commit or rollbackレコードがあるならこのトランザクションをundoする必要はない
      } else if (!finishedTxs.contains(rec.txNumber())) {
        rec.undo(tx);
      }
    }
  }
}
