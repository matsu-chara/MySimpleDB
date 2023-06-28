package tx;

import buffer.Buffer;
import buffer.BufferMgr;
import file.BlockId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BufferList {
  private Map<BlockId, Buffer> buffers = new HashMap<>();
  private List<BlockId> pins = new ArrayList<>();
  private BufferMgr bm;

  public BufferList(BufferMgr bm) {
    this.bm = bm;
  }

  public Buffer getBuffer(BlockId blk) {
    return buffers.get(blk);
  }

  void pin(BlockId blk) {
    Buffer buff = bm.pin(blk);
    buffers.put(blk, buff);   // buffersは現在pinしているbufferへの参照を持つ。(Transactionから使われるので暗黙に現在のトランザクションでpinされているbuffer一覧となる）
    pins.add(blk);
  }

  void unpin(BlockId blk) {
    Buffer buff = buffers.get(blk);
    bm.unpin(buff);
    pins.remove(blk);
    if (!pins.contains(blk)) { // pinが0件になったらbuffersからの参照を消す
      buffers.remove(blk);
    }
  }

  void unpinAll() {
    for (BlockId blk : pins) {
      Buffer buff = buffers.get(blk);
      bm.unpin(buff);
    }
    buffers.clear();
    pins.clear();
  }
}
