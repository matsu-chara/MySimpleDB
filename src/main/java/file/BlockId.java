package file;

import java.util.Objects;

/** ブロックを特定するためのID ブロック自体のサイズはOS依存 */
public class BlockId {
  private String filename;
  private int blknum;

  public BlockId(String filename, int blknum) {
    this.filename = filename;
    this.blknum = blknum;
  }

  public String filename() {
    return filename;
  }

  public int number() {
    return blknum;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    var blockId = (BlockId) o;
    return blknum == blockId.blknum && Objects.equals(filename, blockId.filename);
  }

  @Override
  public int hashCode() {
    return Objects.hash(filename, blknum);
  }

  @Override
  public String toString() {
    return "[main.file " + filename + ", block " + blknum + "]";
  }
}
