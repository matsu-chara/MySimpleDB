package tx.recovery;

import file.Page;
import log.LogMgr;
import tx.Transaction;

public class CheckpointRecord implements LogRecord {

  @Override
  public int op() {
    return CHECKPOINT;
  }

  @Override
  public int txNumber() {
    return -1; // dummy
  }

  @Override
  public void undo(Transaction tx) {
    // do nothing
  }

  public String toString() {
    return "<CHECKPOINT>";
  }

  public static int writeToLog(LogMgr lm) {
    var rec = new byte[Integer.BYTES];
    var p = new Page(rec);
    p.setInt(0, CHECKPOINT);
    return lm.append(rec);
  }
}
