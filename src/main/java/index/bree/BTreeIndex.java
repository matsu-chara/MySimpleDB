package index.bree;

import static java.sql.Types.INTEGER;

import file.BlockId;
import index.Index;
import query.Constant;
import record.Layout;
import record.RID;
import record.Schema;
import tx.Transaction;

public class BTreeIndex implements Index {
  private Transaction tx;
  private Layout dirlayout, leafLayout;
  private String leaftbl;
  private BTreeLeaf leaf = null;
  private BlockId rootblk;

  public BTreeIndex(Transaction tx, String idxname, Layout leaflayout) {
    this.tx = tx;
    this.leaftbl = idxname + "leaf";
    this.leafLayout = leaflayout;
    if (tx.size(leaftbl) == 0) {
      var blk = tx.append(leaftbl);
      var node = new BTPage(tx, blk, leaflayout);
      node.format(blk, -1);
    }

    var dirsch = new Schema();
    dirsch.add("block", leaflayout.schema());
    dirsch.add("dataval", leaflayout.schema());
    var dirtbl = idxname + "dir";
    dirlayout = new Layout(dirsch);
    rootblk = new BlockId(dirtbl, 0);
    if (tx.size(dirtbl) == 0) {
      // create new root block
      var blk = tx.append(dirtbl);
      var node = new BTPage(tx, blk, dirlayout);
      node.format(rootblk, 0);

      // insert initial directory entry
      var fldtype = dirsch.type("dataval");
      var minval = (fldtype == INTEGER) ? new Constant(Integer.MIN_VALUE) : new Constant("");
      node.insertDir(0, minval, 0);
      node.close();
    }
  }

  @Override
  public void beforeFirst(Constant searchkey) {
    close();
    var root = new BTreeDir(tx, rootblk, dirlayout);
    var blknum = root.search(searchkey);
    root.close();
    var leafblk = new BlockId(leaftbl, blknum);
    leaf = new BTreeLeaf(tx, leafblk, leafLayout, searchkey);
  }

  @Override
  public boolean next() {
    return leaf.next();
  }

  @Override
  public RID getRid() {
    return leaf.getDataRid();
  }

  @Override
  public void insert(Constant dataval, RID datarid) {
    beforeFirst(dataval);
    var e = leaf.insert(datarid);
    leaf.close();
    if (e == null) {
      return;
    }
    var root = new BTreeDir(tx, rootblk, dirlayout);
    var e2 = root.insert(e);
    if (e2 != null) {
      root.makeNewRoot(e2);
    }
    root.close();
  }

  @Override
  public void delete(Constant dataval, RID datarid) {
    beforeFirst(dataval);
    leaf.delete(datarid);
    leaf.close();
  }

  @Override
  public void close() {
    if (leaf != null) {
      leaf.close();
    }
  }

  public static int searchCost(int numblocks, int rpb) {
    return 1 + (int) (Math.log(numblocks) / Math.log(rpb));
  }
}
