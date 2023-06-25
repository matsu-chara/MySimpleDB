package tx.concurrency;


import file.BlockId;

import java.util.HashMap;
import java.util.Map;

public class ConcurrencyMgr {
    private static LockTable lockTbl = new LockTable();

    private Map<BlockId, String> locks = new HashMap<>();

    public void slock(BlockId blk) {
        if (locks.get(blk) == null) {
            lockTbl.slock(blk);
            locks.put(blk, "S");
        }
    }

    public void xlock(BlockId blk) {
        if(!hasXlock(blk)) {
            slock(blk); // shared lockをとってからexclusive lockにupgradeする
            lockTbl.xlock(blk);
            locks.put(blk, "X");
        }
    }

    public void release() {
        for(BlockId blk: locks.keySet()) {
            lockTbl.unlock(blk);
        }
        locks.clear();
    }

    private boolean hasXlock(BlockId blk) {
        String lockType = locks.get(blk);
        return lockType != null && lockType.equals("X");
    }
}
