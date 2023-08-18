package index.bree;

import static java.sql.Types.INTEGER;

import file.BlockId;
import query.Constant;
import record.Layout;
import record.RID;
import tx.Transaction;

public class BTPage {
  private Transaction tx;
  private BlockId currentblk;
  private Layout layout;

  public BTPage(Transaction tx, BlockId currentblk, Layout layout) {
    this.tx = tx;
    this.currentblk = currentblk;
    this.layout = layout;
    tx.pin(currentblk);
  }

  public int findSlotBefore(Constant searchKey) {
    var slot = 0;
    while (slot < getNumRecs() && getDataVal(slot).compareTo(searchKey) < 0) {
      slot++;
    }
    return slot - 1;
  }

  public void close() {
    if (currentblk != null) {
      tx.unpin(currentblk);
    }
    currentblk = null;
  }

  public boolean isFull() {
    return slotpos(getNumRecs() + 1) >= tx.blockSize();
  }

  public BlockId split(int splitpos, int flag) {
    var newblk = appendNew(flag);
    var newpage = new BTPage(tx, newblk, layout);
    transferRecs(splitpos, newpage);
    newpage.setFlag(flag);
    newpage.close();
    return newblk;
  }

  public Constant getDataVal(int slot) {
    return getVal(slot, "dataval");
  }

  public int getFlag() {
    return tx.getInt(currentblk, 0);
  }

  public void setFlag(int val) {
    tx.setInt(currentblk, 0, val, true);
  }

  public BlockId appendNew(int flag) {
    var blk = tx.append(currentblk.filename());
    tx.pin(blk);
    format(blk, flag);
    return blk;
  }

  public void format(BlockId blk, int flag) {
    tx.setInt(blk, 0, flag, false);
    tx.setInt(blk, Integer.BYTES, 0, false); // numRecs=0
    var recsize = layout.slotsize();
    for (var pos = 2 * Integer.BYTES; pos + recsize <= tx.blockSize(); pos += recsize) {
      makeDefaultRecord(blk, pos);
    }
  }

  public void makeDefaultRecord(BlockId blk, int pos) {
    for (var fldname : layout.schema().fields()) {
      int offset = layout.offset(fldname);
      if (layout.schema().type(fldname) == INTEGER) {
        tx.setInt(blk, pos + offset, 0, false);
      } else {
        tx.setString(blk, pos + offset, "", false);
      }
    }
  }

  public int getChildNum(int slot) {
    return getInt(slot, "block");
  }

  public void insertDir(int slot, Constant val, int blknum) {
    insert(slot);
    setVal(slot, "dataval", val);
    setInt(slot, "block", blknum);
  }

  public RID getDataRid(int slot) {
    return new RID(getInt(slot, "block"), getInt(slot, "id"));
  }

  public void insertLeaf(int slot, Constant val, RID rid) {
    insert(slot);
    setVal(slot, "dataval", val);
    setInt(slot, "block", rid.blknum());
    setInt(slot, "id", rid.slot());
  }

  public void delete(int slot) {
    for (var i = slot + 1; i < getNumRecs(); i++) {
      copyRecord(i, i - 1);
    }
    setNumRecs(getNumRecs() - 1);
  }

  public int getNumRecs() {
    return tx.getInt(currentblk, Integer.BYTES);
  }

  private int getInt(int slot, String fldname) {
    var pos = fldpos(slot, fldname);
    return tx.getInt(currentblk, pos);
  }

  private String getString(int slot, String fldname) {
    var pos = fldpos(slot, fldname);
    return tx.getString(currentblk, pos);
  }

  private Constant getVal(int slot, String fldname) {
    var type = layout.schema().type(fldname);
    if (type == INTEGER) {
      return new Constant(getInt(slot, fldname));
    } else {
      return new Constant(getString(slot, fldname));
    }
  }

  private void setInt(int slot, String fldname, int val) {
    var pos = fldpos(slot, fldname);
    tx.setInt(currentblk, pos, val, true);
  }

  private void setString(int slot, String fldname, String val) {
    var pos = fldpos(slot, fldname);
    tx.setString(currentblk, pos, val, true);
  }

  private void setVal(int slot, String fldname, Constant val) {
    var type = layout.schema().type(fldname);
    if (type == INTEGER) {
      setInt(slot, fldname, val.asInt());
    } else {
      setString(slot, fldname, val.asString());
    }
  }

  private void setNumRecs(int n) {
    tx.setInt(currentblk, Integer.BYTES, n, true);
  }

  private void insert(int slot) {
    for (var i = getNumRecs(); i > slot; i--) {
      copyRecord(i - 1, i);
    }
    setNumRecs(getNumRecs() + 1);
  }

  private void copyRecord(int from, int to) {
    var sch = layout.schema();
    for (var fldname : sch.fields()) {
      setVal(to, fldname, getVal(from, fldname));
    }
  }

  private void transferRecs(int slot, BTPage dest) {
    var destslot = 0;
    while (slot < getNumRecs()) {
      dest.insert(destslot);
      var sch = layout.schema();
      for (var fldname : sch.fields()) {
        dest.setVal(destslot, fldname, getVal(slot, fldname));
      }
      delete(slot);
      destslot++;
    }
  }

  private int fldpos(int slot, String fldname) {
    int offset = layout.offset(fldname);
    return slotpos(slot) + offset;
  }

  private int slotpos(int slot) {
    var slotsize = layout.slotsize();
    return Integer.BYTES + Integer.BYTES + (slot * slotsize);
  }
}
