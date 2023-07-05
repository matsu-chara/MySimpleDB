package tx.recovery;

import static tx.recovery.LogRecord.CHECKPOINT;
import static tx.recovery.LogRecord.COMMIT;
import static tx.recovery.LogRecord.ROLLBACK;
import static tx.recovery.LogRecord.START;

import buffer.Buffer;
import buffer.BufferMgr;
import java.util.ArrayList;
import java.util.Collection;
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
    var lsn = CommitRecord.writeToLog(lm, txnum);
    lm.flush(lsn);
  }

  public void rollback() {
    doRollBack();
    bm.flushAll(txnum);
    var lsn = RollbackRecord.writeToLog(lm, txnum);
    lm.flush(lsn);
  }

  public void recover() {
    doRecover();
    bm.flushAll(txnum);
    var lsn = CheckpointRecord.writeToLog(lm);
    lm.flush(lsn);
  }

  // newvalは今回の実装だと利用してないのでサンプルコードにはあるけど消してみた
  // ただsetIntとsetStringを正確に呼び分けないとランタイムエラーになるので悪手だったかもsrc/main/java/tx/recovery/RecoveryMgr.java
  public int setInt(Buffer buff, int offset) {
    var oldval = buff.contents().getInt(offset);
    var blk = buff.block();
    return SetIntRecord.writeToLog(lm, txnum, blk, offset, oldval);
  }

  public int setString(Buffer buff, int offset) {
    var oldval = buff.contents().getString(offset);
    var blk = buff.block();
    return SetStringRecord.writeToLog(lm, txnum, blk, offset, oldval);
  }

  private void doRollBack() {
    var iter = lm.iterator();
    while (iter.hasNext()) {
      var bytes = iter.next();
      var rec = LogRecord.createLogRecord(bytes);
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
    var iter = lm.iterator();
    while (iter.hasNext()) {
      var bytes = iter.next();
      var rec = LogRecord.createLogRecord(bytes);
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
