package tx.concurrency;

import file.BlockId;
import java.util.HashMap;
import java.util.Map;

public class LockTable {
  private static int DEFAULT_MAX_TIME = 10000; // 10sec
  private long maxTime;

  private Map<BlockId, Integer> locks = new HashMap<>();

  public LockTable() {
    this.maxTime = DEFAULT_MAX_TIME;
  }

  public LockTable(long maxTime) {
    this.maxTime = maxTime;
  }

  synchronized void sLock(BlockId blk) {
    try {
      long starttime = System.currentTimeMillis();
      while (hasXLock(blk) && !waitingTooLong(starttime)) {
        wait(maxTime);
      }
      if (hasXLock(blk)) {
        throw new LockAbortException();
      }
      int val = getLockVal(blk); // will not be negative
      locks.put(blk, val + 1);
    } catch (InterruptedException e) {
      throw new LockAbortException();
    }
  }

  synchronized void xLock(BlockId blk) {
    try {
      long starttime = System.currentTimeMillis();
      while (hasOtherSLocks(blk) && !waitingTooLong(starttime)) {
        wait(maxTime);
      }
      if (hasOtherSLocks(blk)) {
        throw new LockAbortException();
      }
      locks.put(blk, -1);
    } catch (InterruptedException e) {
      throw new LockAbortException();
    }
  }

  synchronized void unlock(BlockId blk) {
    int val = getLockVal(blk);
    if (val > 1) {
      locks.put(blk, val - 1);
    } else {
      locks.remove(blk);
      notifyAll();
    }
  }

  private boolean hasXLock(BlockId blk) {
    return getLockVal(blk) < 0; // マイナスならexclusive lock
  }

  private boolean hasOtherSLocks(BlockId blk) {
    return getLockVal(blk) > 1;
  }

  private boolean waitingTooLong(long starttime) {
    return System.currentTimeMillis() - starttime > maxTime;
  }

  private int getLockVal(BlockId blk) {
    Integer ival = locks.get(blk);
    return (ival == null) ? 0 : ival;
  }
}
