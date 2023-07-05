package log;

import file.BlockId;
import file.FileMgr;
import file.Page;
import java.util.Iterator;

public class LogMgr {
  private FileMgr fm;
  private String logfile;
  private Page logpage;
  private BlockId currentblk;

  private int latestLSN = 0; // log sequence number
  private int lastSavedLSN = 0;

  public LogMgr(FileMgr fm, String logfile) {
    this.fm = fm;
    this.logfile = logfile;
    var b = new byte[fm.blockSize()];
    logpage = new Page(b);

    var logsize = fm.length(logfile);
    if (logsize == 0) {
      currentblk = appendNewBlock();
    } else {
      currentblk = new BlockId(logfile, logsize - 1);
      fm.read(currentblk, logpage);
    }
  }

  public void flush(int lsn) {
    if (lsn >= lastSavedLSN) {
      flush();
    }
  }

  public Iterator<byte[]> iterator() {
    flush(); // read時にメモリ内のPageをflushする
    return new LogIterator(fm, currentblk);
  }

  public synchronized int append(byte[] logrec) {
    var boundary = logpage.getInt(0); // logPage先頭に書いてあるboundaryを読み込む
    var recsize = logrec.length;
    var bytesneeded = recsize + Integer.BYTES;

    // logPage内にこのエントリが収まるか判断(Integer.BYTESはboundary保存用の領域分を確保するためにとってある）
    if (boundary - bytesneeded < Integer.BYTES) {
      // 書き込めないので次のページに行く
      flush();
      currentblk = appendNewBlock();
      boundary = logpage.getInt(0);
    }
    var recpos = boundary - bytesneeded; // logRecord は Page 末尾から書くことになる（logIterator が逆順にたどるときに便利）
    logpage.setBytes(recpos, logrec);
    logpage.setInt(0, recpos); // logPage先頭に書いてあるboundaryを新しいものに更新
    latestLSN += 1; // log sequence numberを一つ追加
    return latestLSN;
  }

  private BlockId appendNewBlock() {
    var blk = fm.append(logfile); // 1ブロック追加
    logpage.setInt(0, fm.blockSize()); // ブロックサイズを初期boundaryとしてPage先頭に書き込む
    fm.write(blk, logpage); // ファイルにPageを書き込む
    return blk;
  }

  private void flush() {
    fm.write(currentblk, logpage);
    lastSavedLSN = latestLSN;
  }
}
