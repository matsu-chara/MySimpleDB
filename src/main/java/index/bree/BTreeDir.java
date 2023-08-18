package index.bree;

import file.BlockId;
import query.Constant;
import record.Layout;
import tx.Transaction;

public class BTreeDir {
  private Transaction tx;
  private Layout layout;
  private BTPage contents;
  private String filename;

  public BTreeDir(Transaction tx, BlockId blk, Layout layout) {
    this.tx = tx;
    this.layout = layout;
    contents = new BTPage(tx, blk, layout);
    filename = blk.filename();
  }

  public void close() {
    contents.close();
  }

  public int search(Constant searchKey) {
    var childblk = findChildBlock(searchKey);
    while (contents.getFlag() > 0) {
      contents.close();
      contents = new BTPage(tx, childblk, layout);
      childblk = findChildBlock(searchKey);
    }
    return childblk.blknum();
  }

  public void makeNewRoot(DirEntry e) {
    var firstval = contents.getDataVal(0);
    var level = contents.getFlag();
    var newblk = contents.split(0, level); // transfer all the records
    var oldroot = new DirEntry(firstval, newblk.blknum());
    insertEntry(oldroot);
    insertEntry(e);
    contents.setFlag(level + 1);
  }

  public DirEntry insert(DirEntry e) {
    if (contents.getFlag() == 0) {
      return insertEntry(e);
    }
    var childblk = findChildBlock(e.dataval());
    var child = new BTreeDir(tx, childblk, layout);
    var myentry = child.insert(e);
    child.close();
    return (myentry != null) ? insertEntry(e) : null;
  }

  public DirEntry insertEntry(DirEntry e) {
    var newslot = 1 + contents.findSlotBefore(e.dataval());
    contents.insertDir(newslot, e.dataval(), e.blocknumber());
    if (!contents.isFull()) {
      return null;
    }

    // start split
    var level = contents.getFlag();
    var splitpos = contents.getNumRecs() / 2;
    var splitval = contents.getDataVal(splitpos);
    var newblk = contents.split(splitpos, level);
    return new DirEntry(splitval, newblk.blknum());
  }

  private BlockId findChildBlock(Constant searchKey) {
    var slot = contents.findSlotBefore(searchKey);
    if (contents.getDataVal(slot + 1).equals(searchKey)) {
      slot++;
    }
    var blknum = contents.getChildNum(slot);
    return new BlockId(filename, blknum);
  }
}
