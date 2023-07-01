package tx.concurrency;

import file.BlockId;
import java.util.HashMap;
import java.util.Map;

public class ConcurrencyMgr {
  private static LockTable lockTbl = new LockTable();

  private Map<BlockId, String> locks = new HashMap<>();

  public void sLock(BlockId blk) {
    if (locks.get(blk) == null) {
      lockTbl.sLock(blk);
      locks.put(blk, "S");
    }
  }

  public void xLock(BlockId blk) {
    if (!hasXLock(blk)) {
      sLock(blk); // shared lockをとってからexclusive lockにupgradeする
      lockTbl.xLock(blk);
      locks.put(blk, "X");
    }
  }

  public void release() {
    for (BlockId blk : locks.keySet()) {
      lockTbl.unlock(blk);
    }
    locks.clear();
  }

  private boolean hasXLock(BlockId blk) {
    String lockType = locks.get(blk);
    return lockType != null && lockType.equals("X");
  }
}
