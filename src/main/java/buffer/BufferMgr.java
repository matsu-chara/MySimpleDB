package buffer;

import file.BlockId;
import file.FileMgr;
import java.util.Arrays;
import log.LogMgr;

public class BufferMgr {
  private Buffer[] bufferpool;

  private int numAvailable;

  private long maxTime;

  private static final long DEFAULT_MAX_TIME = 10000; // 10 seconds

  public BufferMgr(FileMgr fm, LogMgr lm, int numBuffers) {
    this.bufferpool = new Buffer[numBuffers];
    this.numAvailable = numBuffers;
    this.maxTime = DEFAULT_MAX_TIME;

    for (var i = 0; i < numBuffers; i++) {
      bufferpool[i] = new Buffer(fm, lm);
    }
  }

  public BufferMgr(FileMgr fm, LogMgr lm, int numBuffers, long maxTime) {
    this.bufferpool = new Buffer[numBuffers];
    this.numAvailable = numBuffers;
    this.maxTime = maxTime;

    for (var i = 0; i < numBuffers; i++) {
      bufferpool[i] = new Buffer(fm, lm);
    }
  }

  public synchronized int available() {
    return numAvailable;
  }

  public synchronized void flushAll(int txnum) {
    for (var buffer : bufferpool) {
      if (buffer.modifyingTx() == txnum) {
        buffer.flush();
      }
    }
  }

  public synchronized void unpin(Buffer buff) {
    buff.unpin();
    if (!buff.isPinned()) { // なんどもpinされているbufferの場合はpinned状態である可能性がある
      numAvailable++;
      notifyAll(); // pinでwaitされる
    }
  }

  public synchronized Buffer pin(BlockId blk) {
    try {
      var timestamp = System.currentTimeMillis();
      var buff = tryToPin(blk);
      while (buff == null && !waitingTooLong(timestamp)) {
        wait(maxTime); // unpinでnotifyAllされる
        buff = tryToPin(blk);
      }
      if (buff == null) {
        throw new BufferAbortException();
      }
      return buff;
    } catch (InterruptedException e) {
      throw new BufferAbortException();
    }
  }

  private boolean waitingTooLong(long starttime) {
    return System.currentTimeMillis() - starttime > maxTime;
  }

  private Buffer tryToPin(BlockId blk) {
    var buff = findExistingBuffer(blk);
    if (buff == null) {
      buff = chooseUnpinnedBuffer();
      if (buff == null) {
        return null;
      }
      buff.assignToBlock(blk);
    }

    // 既存バッファから選択された場合すでにpinされている可能性がある。
    if (!buff.isPinned()) {
      numAvailable--;
    }

    buff.pin(); // pins変数をインクリメントしてpinされた回数をカウントしているので既存バッファから選択されていた場合でも毎回pinする
    return buff;
  }

  private Buffer findExistingBuffer(BlockId blk) {
    for (var buff : bufferpool) {
      var b = buff.block();
      if (b != null && b.equals(blk)) {
        return buff;
      }
    }
    return null;
  }

  /** nullable */
  private Buffer chooseUnpinnedBuffer() {
    return Arrays.stream(bufferpool).filter(buffer -> !buffer.isPinned()).findFirst().orElse(null);
  }
}
