package file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class FileMgr {
  private File dbDirectory;
  private int blockSize;
  private boolean isNew; // dbが新規かどうか
  private Map<String, RandomAccessFile> openFiles = new HashMap<>();

  public FileMgr(File dbDirectory, int blockSize) {
    this.dbDirectory = dbDirectory;
    this.blockSize = blockSize;
    isNew = !dbDirectory.exists();

    // 新規ならディレクトリを作る
    if (isNew) {
      dbDirectory.mkdirs();
    }

    for (String filename : Objects.requireNonNull(dbDirectory.list())) {
      // temporaryテーブルは初期化時に削除する
      if (filename.startsWith("temp")) {
        deleteFile(filename);
      }
    }
  }

  /** blockの内容をblockに対応するfileからpageに読み込む */
  public synchronized void read(BlockId blk, Page p) {
    try {
      RandomAccessFile f = getFile(blk.filename());
      f.seek((long) blk.number() * blockSize);
      f.getChannel().read(p.contents());
    } catch (IOException e) {
      throw new RuntimeException("cannot read block " + blk);
    }
  }

  /** Pageの内容をblockに対応するfileに書きこむ */
  public synchronized void write(BlockId blk, Page p) {
    try {
      RandomAccessFile f = getFile(blk.filename());
      f.seek((long) blk.number() * blockSize);
      f.getChannel().write(p.contents());
    } catch (IOException e) {
      throw new RuntimeException("cannot write block " + blk);
    }
  }

  public synchronized BlockId append(String filename) {
    int newblknum = length(filename);
    BlockId blk = new BlockId(filename, newblknum);
    byte[] b = new byte[blockSize];
    try {
      RandomAccessFile f = getFile(filename);
      f.seek((long) blk.number() * blockSize); // 末尾までseek
      f.write(b); // 1ブロック分だけ書き込む
    } catch (IOException e) {
      throw new RuntimeException("cannot append block " + blk);
    }
    return blk;
  }

  /** ファイルに含まれるブロック数を返す */
  public int length(String filename) {
    try {
      RandomAccessFile f = getFile(filename);
      return (int) (f.length() / blockSize);
    } catch (IOException e) {
      throw new RuntimeException("cannot access " + filename);
    }
  }

  public boolean isNew() {
    return isNew;
  }

  public int blockSize() {
    return blockSize;
  }

  private RandomAccessFile getFile(String filename) throws IOException {
    RandomAccessFile f = openFiles.get(filename);
    if (f == null) {
      File dbTable = new File(dbDirectory, filename);
      f = new RandomAccessFile(dbTable, "rws");
      openFiles.put(filename, f);
    }
    return f;
  }

  /**
   * this is for testing
   */
  public void deleteFile(String filename) {
    new File(dbDirectory, filename).delete();
  }
}
