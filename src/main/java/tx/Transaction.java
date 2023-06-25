package tx;

import file.BlockId;

public class Transaction {
  public void pin() {}

  public void unpin(BlockId blk) {}

  public void setInt(BlockId blk, int offset, int val, boolean okToLog) {}

  public void setString(BlockId blk, int offset, String val, boolean okToLog) {}
}
