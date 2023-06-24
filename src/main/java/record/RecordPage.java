package record;

import static java.sql.Types.INTEGER;

import file.BlockId;
import tx.Transaction;

public class RecordPage {
  public static int EMPTY = 0, USED = 1;

  private Transaction tx;

  private BlockId blk;

  private Layout layout;

  public RecordPage(Transaction tx, BlockId blk, Layout layout) {
    this.tx = tx;
    this.blk = blk;
    this.layout = layout;
    tx.pin(blk);
  }

  public int getInt(int slot, String fldname) {
    var fldpos = offset(slot) + layout.offsets(fldname);
    return tx.getInt(blk, fldpos);
  }

  public String getString(int slot, String fldname) {
    var fldpos = offset(slot) + layout.offsets(fldname);
    return tx.getString(blk, fldpos);
  }

  public void setInt(int slot, String fldname, int val) {
    var fldpos = offset(slot) + layout.offsets(fldname);
    tx.setInt(blk, fldpos, val, true);
  }

  public void setString(int slot, String fldname, String val) {
    var fldpos = offset(slot) + layout.offsets(fldname);
    tx.setString(blk, fldpos, val, true);
  }

  /** 1blockをschemaでformatする。block先頭にはempty/usedフラグが存在する。 */
  public void format() {
    var slot = 0;
    while (isValidSlot(slot)) {
      tx.setInt(blk, offset(slot), EMPTY, false);
      var sch = layout.schema();
      for (var fldname : sch.fields()) {
        var flodpos = offset(slot) + layout.offsets(fldname);
        if (sch.type(fldname) == INTEGER) {
          tx.setInt(blk, flodpos, 0, false);
        } else {
          tx.setString(blk, flodpos, "", false);
        }
      }
      slot++;
    }
  }

  public int nextAfter(int slot) {
    return searchAfter(slot, USED);
  }

  public void delete(int slot) {
    setFlag(slot, EMPTY);
  }

  public int insertAfter(int slot) {
    var newslot = searchAfter(slot, EMPTY);
    if (newslot >= 0) {
      setFlag(newslot, USED);
    }
    return newslot;
  }

  public BlockId block() {
    return blk;
  }

  private void setFlag(int slot, int flag) {
    tx.setInt(blk, offset(slot), flag, true);
  }

  private int searchAfter(int slot, int flag) {
    slot++;
    while (isValidSlot(slot)) {
      if (tx.getInt(blk, offset(slot)) == flag) {
        return slot;
      }
      slot++;
    }
    return -1;
  }

  private boolean isValidSlot(int slot) {
    return offset(slot + 1) <= tx.blockSize();
  }

  private int offset(int slot) {
    return slot * layout.slotsize();
  }
}
