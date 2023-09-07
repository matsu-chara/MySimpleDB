package multibuffer;

import query.Constant;
import query.ProductScan;
import query.Scan;
import record.Layout;
import tx.Transaction;

public class MultibufferProductScan implements Scan {
  private Transaction tx;
  private Scan lhsscan, rhsscan = null, prodscan;

  private String filename;
  private Layout layout;
  private int chunksize, nextblknum, filesize;

  public MultibufferProductScan(Transaction tx, Scan lhsscan, String tblname, Layout layout) {
    this.tx = tx;
    this.lhsscan = lhsscan;
    this.filename = tblname + ".tbl";
    this.layout = layout;
    filesize = tx.size(filename);
    chunksize = BufferNeeds.bestFactor(tx.availableBuffers(), filesize);
    beforeFirst();
  }

  @Override
  public void beforeFirst() {
    nextblknum = 0;
    useNextChunk();
  }

  @Override
  public boolean next() {
    while (!prodscan.next()) {
      if (!useNextChunk()) {
        return false;
      }
    }
    return false;
  }

  @Override
  public int getInt(String fldname) {
    return prodscan.getInt(fldname);
  }

  @Override
  public String getString(String fldname) {
    return prodscan.getString(fldname);
  }

  @Override
  public Constant getVal(String fldname) {
    return prodscan.getVal(fldname);
  }

  @Override
  public boolean hasField(String fldname) {
    return prodscan.hasField(fldname);
  }

  @Override
  public void close() {
    prodscan.close();
  }

  private boolean useNextChunk() {
    if (nextblknum >= filesize) {
      return false;
    }
    if (rhsscan != null) {
      rhsscan.close();
    }
    var end = nextblknum + chunksize - 1;
    if (end >= filesize) {
      end = filesize - 1;
    }
    rhsscan = new ChunkScan(tx, filename, layout, nextblknum, end);
    lhsscan.beforeFirst();
    prodscan = new ProductScan(lhsscan, rhsscan);
    nextblknum = end + 1;
    return true;
  }
}
