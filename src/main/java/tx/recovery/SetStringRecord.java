package tx.recovery;

import file.BlockId;
import file.Page;
import log.LogMgr;
import tx.Transaction;

public class SetStringRecord implements LogRecord {
  private int txnum, offset;
  private String val;
  private BlockId blk;

  public SetStringRecord(Page p) {
    // txnum + filename + blockNum + offset + value の順に入っている
    var tpos = Integer.BYTES;
    this.txnum = p.getInt(tpos);

    var fpos = tpos + Integer.BYTES;
    var filename = p.getString(fpos);

    var bpos = fpos + Page.maxLength(filename.length());
    var blknum = p.getInt(bpos);
    blk = new BlockId(filename, blknum);

    var opos = bpos + Integer.BYTES;
    offset = p.getInt(opos);

    var vpos = opos + Integer.BYTES;
    val = p.getString(vpos);
  }

  @Override
  public int op() {
    return SETSTRING;
  }

  @Override
  public int txNumber() {
    return txnum;
  }

  @Override
  public String toString() {
    return "<SETSTRING "
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
    tx.setString(blk, offset, val, false); // undoをlogしないように注意
    tx.unpin(blk);
  }

  public static int writeToLog(LogMgr lm, int txnum, BlockId blk, int offset, String val) {
    var tpos = Integer.BYTES;
    var fpos = tpos + Integer.BYTES;
    var bpos = fpos + Page.maxLength(blk.filename().length());
    var opos = bpos + Integer.BYTES;
    var vpos = opos + Integer.BYTES;
    var reclen = vpos + Page.maxLength(val.length());
    var rec = new byte[reclen];
    var p = new Page(rec);

    p.setInt(0, SETSTRING);
    p.setInt(tpos, txnum);
    p.setString(fpos, blk.filename());
    p.setInt(bpos, blk.blknum());
    p.setInt(opos, offset);
    p.setString(vpos, val);
    return lm.append(rec);
  }
}
