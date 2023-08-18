package index.bree;

import query.Constant;

public class DirEntry {
  private Constant dataval;
  private int blocknum;

  public DirEntry(Constant dataval, int blocknum) {
    this.dataval = dataval;
    this.blocknum = blocknum;
  }

  public Constant dataval() {
    return dataval;
  }

  public int blocknumber() {
    return blocknum;
  }
}
