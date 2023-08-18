package index.bree;

import file.BlockId;
import query.Constant;
import record.Layout;
import record.RID;
import tx.Transaction;

public class BTreeLeaf {
  private Transaction tx;
  private Layout layout;
  private Constant searchKey;
  private BTPage contents;
  private int currentslot;
  private String filename;

  public BTreeLeaf(Transaction tx, BlockId blk, Layout layout, Constant searchKey) {
    this.tx = tx;
    this.layout = layout;
    this.searchKey = searchKey;
    this.contents = new BTPage(tx, blk, layout);
    currentslot = contents.findSlotBefore(searchKey);
    filename = blk.filename();
  }

  public void close() {
    contents.close();
  }

  public boolean next() {
    currentslot++;
    if (currentslot >= contents.getNumRecs()) {
      return tryOverflow();
    } else if (contents.getDataVal(currentslot).equals(searchKey)) {
      return true;
    } else {
      return tryOverflow();
    }
  }

  public RID getDataRid() {
    return contents.getDataRid(currentslot);
  }

  public void delete(RID datarid) {
    while (next()) {
      if (getDataRid().equals(datarid)) {
        contents.delete(currentslot);
        return;
      }
    }
  }

  // (RID, searchKey)が書き込まれる
  public DirEntry insert(RID datarid) {
    if (contents.getFlag() >= 0 && contents.getDataVal(0).compareTo(searchKey) > 0) {
      var firstval = contents.getDataVal(0);
      var newblk = contents.split(0, contents.getFlag());
      currentslot = 0;
      contents.setFlag(-1);
      contents.insertLeaf(currentslot, searchKey, datarid);
      return new DirEntry(firstval, newblk.blknum());
    }

    currentslot++;
    contents.insertLeaf(currentslot, searchKey, datarid);
    if (!contents.isFull()) {
      return null;
    }

    // split
    var firstkey = contents.getDataVal(0);
    var lastKey = contents.getDataVal(contents.getNumRecs() - 1);
    if (lastKey.equals(firstkey)) {
      // overflow blockを作る。先頭１レコード以外はoveflow blockに移動する
      var newblk = contents.split(1, contents.getFlag());
      contents.setFlag(newblk.blknum());
      return null;
    } else {
      var splitpos = contents.getNumRecs() / 2;
      var splitkey = contents.getDataVal(splitpos);
      if (splitkey.equals(firstkey)) {
        // move right, looking for the next key
        while (contents.getDataVal(splitpos).equals(splitkey)) {
          splitpos++;
        }
        splitkey = contents.getDataVal(splitpos);
      } else {
        while (contents.getDataVal(splitpos - 1).equals(searchKey)) {
          splitpos--;
        }
      }
      var newblk = contents.split(splitpos, -1);
      return new DirEntry(splitkey, newblk.blknum());
    }
  }

  private boolean tryOverflow() {
    var firstkey = contents.getDataVal(0);
    var flag = contents.getFlag();
    if (!searchKey.equals(firstkey) || flag < 0) {
      return false;
    }
    contents.close();
    var nextblk = new BlockId(filename, flag);
    contents = new BTPage(tx, nextblk, layout);
    currentslot = 0;
    return true;
  }
}
