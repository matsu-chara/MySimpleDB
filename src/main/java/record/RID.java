package record;

public record RID(int blknum, int slot) {

  @Override
  public String toString() {
    return "RID{" + "blknum=" + blknum + ", slot=" + slot + '}';
  }
}
