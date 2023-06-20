package log;

import file.BlockId;
import file.FileMgr;
import file.Page;
import java.util.Iterator;

public class LogIterator implements Iterator<byte[]> {
  private FileMgr fm;
  private BlockId blk;

  private Page p;
  private int currentpos;
  private int boundary;

  public LogIterator(FileMgr fm, BlockId blk) {
    this.fm = fm;
    this.blk = blk;
    byte[] b = new byte[fm.blockSize()];
    p = new Page(b);
    moveToBlock(blk);
  }

  @Override
  public boolean hasNext() {
    return currentpos < fm.blockSize() || blk.number() > 0; // blockNumを逆順に辿っていくので0に到達したら終わり
  }

  @Override
  public byte[] next() {
    if (currentpos == fm.blockSize()) {
      blk = new BlockId(blk.filename(), blk.number() - 1); // blockNumは1ずつ増えていくので逆順にたどるために1ずつ減らしている
      moveToBlock(blk);
    }
    byte[] rec = p.getBytes(currentpos); // レコードのlength(Int) + contentが読み込まれるがdecodeせずに返す(Pageに変換できるのでPageにやらせればよい）
    currentpos += Integer.BYTES + rec.length;
    return rec;
  }

  private void moveToBlock(BlockId blk) {
    fm.read(blk, p);
    boundary = p.getInt(0);
    currentpos = boundary;
  }
}
