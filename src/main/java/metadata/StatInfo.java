package metadata;

public class StatInfo {
  private int numBlocks;
  private int numRecs;

  public StatInfo(int numBlocks, int numRecs) {
    this.numBlocks = numBlocks;
    this.numRecs = numRecs;
  }

  public int blocksAccessed() {
    return numBlocks;
  }

  public int recordsOutput() {
    return numRecs;
  }

  public int distinctValues() {
    return (numRecs / 3); // 実装簡略化のため超適当な推測になっている。
  }
}
