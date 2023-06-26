package tx.recovery;

import file.Page;
import log.LogMgr;
import tx.Transaction;

public class StartRecord implements LogRecord {
  private int txnum;

  public StartRecord(Page p) {
    int tpos = Integer.BYTES;
    txnum = p.getInt(tpos);
  }

  @Override
  public int op() {
    return START;
  }

  @Override
  public int txNumber() {
    return txnum;
  }

  @Override
  public void undo(Transaction tx) {
    // do nothing
  }

  public String toString() {
    return "<START " + txnum + ">";
  }

  public static int writeToLog(LogMgr lm, int txnum) {
    byte[] rec = new byte[2 * Integer.BYTES];
    Page p = new Page(rec);

    p.setInt(0, START);
    p.setInt(Integer.BYTES, txnum);
    return lm.append(rec);
  }
}