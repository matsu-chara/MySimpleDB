package record;

import file.BlockId;
import tx.Transaction;

public class TableScan {
  private Transaction tx;
  private Layout layout;
  private RecordPage rp;
  private String filename;
  private int currentslot;

  public TableScan(Transaction tx, String tblname, Layout layout) {
    this.tx = tx;
    this.layout = layout;
    filename = tblname + ".tbl";
    if (tx.size(filename) == 0) {
      moveToNewBlock();
    } else {
      moveToBlock(0);
    }
  }

  public void close() {
    if (rp != null) {
      tx.unpin(rp.block());
    }
  }

  private void moveToBlock(int blknum) {
    close();
    var blk = new BlockId(filename, blknum);
    rp = new RecordPage(tx, blk, layout);
    currentslot = -1;
  }

  private void moveToNewBlock() {
    close();
    var blk = tx.append(filename);
    rp = new RecordPage(tx, blk, layout);
    rp.format();
    currentslot = -1;
  }

  private boolean atLastBlock() {
    return rp.block().blknum() == tx.size(filename) - 1;
  }
}
