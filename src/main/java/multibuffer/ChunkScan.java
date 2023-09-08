package multibuffer;

import static java.sql.Types.INTEGER;

import file.BlockId;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import query.Constant;
import query.Scan;
import record.Layout;
import record.RecordPage;
import tx.Transaction;

public class ChunkScan implements Scan {
  private List<RecordPage> buffs = new ArrayList<>();
  private Transaction tx;
  private String filename;
  private Layout layout;
  private int startbnum, endbnum, currentbnum;
  private RecordPage rp;
  int currentslot;

  public ChunkScan(Transaction tx, String filename, Layout layout, int startbnum, int endbnum) {
    this.tx = tx;
    this.filename = filename;
    this.layout = layout;
    this.startbnum = startbnum;
    this.endbnum = endbnum;
    IntStream.range(startbnum, endbnum + 1)
        .mapToObj(i -> new BlockId(filename, i))
        .forEach(blk -> buffs.add(new RecordPage(tx, blk, layout)));
    moveToBlock(startbnum);
  }

  @Override
  public void beforeFirst() {
    moveToBlock(startbnum);
  }

  @Override
  public boolean next() {
    currentslot = rp.nextAfter(currentslot);
    while (currentslot < 0) {
      if (currentbnum == endbnum) {
        return false;
      }
      moveToBlock(rp.block().blknum() + 1);
      currentslot = rp.nextAfter(currentslot);
    }
    return true;
  }

  @Override
  public int getInt(String fldname) {
    return rp.getInt(currentslot, fldname);
  }

  @Override
  public String getString(String fldname) {
    return rp.getString(currentslot, fldname);
  }

  @Override
  public Constant getVal(String fldname) {
    if (layout.schema().type(fldname) == INTEGER) {
      return new Constant(getInt(fldname));
    } else {
      return new Constant(getString(fldname));
    }
  }

  @Override
  public boolean hasField(String fldname) {
    return layout.schema().hasField(fldname);
  }

  @Override
  public void close() {
    for (var i = 0; i < buffs.size(); i++) {
      var blk = new BlockId(filename, startbnum + i);
      tx.unpin(blk);
    }
  }

  private void moveToBlock(int blknum) {
    currentbnum = blknum;
    rp = buffs.get(currentbnum - startbnum);
    currentslot = -1;
  }
}
