package tx.recovery;

import file.BlockId;
import file.Page;
import log.LogMgr;
import tx.Transaction;

public class SetIntRecord implements LogRecord {
  private int txnum, offset;
  private int val;
  private BlockId blk;

  public SetIntRecord(Page p) {
    // txnum + filename + blockNum + offset + value の順に入っている
    int tpos = Integer.BYTES;
    this.txnum = p.getInt(tpos);

    int fpos = tpos + Integer.BYTES;
    String filename = p.getString(fpos);

    int bpos = fpos + Page.maxLength(filename.length());
    int blknum = p.getInt(bpos);
    blk = new BlockId(filename, blknum);

    int opos = bpos + Integer.BYTES;
    offset = p.getInt(opos);

    int vpos = opos + Integer.BYTES;
    val = p.getInt(vpos);
  }

  @Override
  public int op() {
    return SETINT;
  }

  @Override
  public int txNumber() {
    return txnum;
  }

  @Override
  public String toString() {
    return "<SETINT "
        + "txnum="
        + txnum
        + ", offset="
        + offset
        + ", val='"
        + val
        + '\''
        + ", blk="
        + blk
        + '>';
  }

  @Override
  public void undo(Transaction tx) {
    tx.pin(blk);
    tx.setInt(blk, offset, val, false); // undoをlogしないように注意
    tx.unpin(blk);
  }

  public static int writeToLog(LogMgr lm, int txnum, BlockId blk, int offset, int val) {
    int tpos = Integer.BYTES;
    int fpos = tpos + Integer.BYTES;
    int bpos = fpos + Page.maxLength(blk.filename().length());
    int opos = bpos + Integer.BYTES;
    int vpos = opos + Integer.BYTES;
    int reclen = vpos + Integer.BYTES;
    byte[] rec = new byte[reclen];
    Page p = new Page(rec);

    p.setInt(0, SETINT);
    p.setInt(tpos, txnum);
    p.setString(fpos, blk.filename());
    p.setInt(bpos, blk.number());
    p.setInt(opos, offset);
    p.setInt(vpos, val);
    return lm.append(rec);
  }
}
