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
      var ok = dbDirectory.mkdirs();
      if (!ok) {
        throw new RuntimeException("failed to create dirs");
      }
    }

    for (var filename : Objects.requireNonNull(dbDirectory.list())) {
      // temporaryテーブルは初期化時に削除する
      if (filename.startsWith("temp")) {
        deleteFile(filename);
      }
    }
  }

  /** blockの内容をblockに対応するfileからpageに読み込む */
  public synchronized void read(BlockId blk, Page p) {
    try {
      var f = getFile(blk.filename());
      f.seek((long) blk.blknum() * blockSize);
      f.getChannel().read(p.contents());
    } catch (IOException e) {
      throw new RuntimeException("cannot read block " + blk);
    }
  }

  /** Pageの内容をblockに対応するfileに書きこむ */
  public synchronized void write(BlockId blk, Page p) {
    try {
      var f = getFile(blk.filename());
      f.seek((long) blk.blknum() * blockSize);
      var contents = p.contents();
      var written = f.getChannel().write(contents);
      if (written != contents.limit()) {
        throw new RuntimeException(
            "The bytes written to the file was not equal to the contents size. written = "
                + written);
      }
    } catch (IOException e) {
      throw new RuntimeException("cannot write block " + blk);
    }
  }

  public synchronized BlockId append(String filename) {
    var newblknum = length(filename);
    var blk = new BlockId(filename, newblknum);
    var b = new byte[blockSize];
    try {
      var f = getFile(filename);
      f.seek((long) blk.blknum() * blockSize); // 末尾までseek
      f.write(b); // 1ブロック分だけ書き込む
    } catch (IOException e) {
      throw new RuntimeException("cannot append block " + blk);
    }
    return blk;
  }

  /** ファイルに含まれるブロック数を返す */
  public int length(String filename) {
    try {
      var f = getFile(filename);
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
    var f = openFiles.get(filename);
    if (f == null) {
      var dbTable = new File(dbDirectory, filename);
      f = new RandomAccessFile(dbTable, "rws");
      openFiles.put(filename, f);
    }
    return f;
  }

  private void deleteFile(String filename) {
    var file = new File(dbDirectory, filename);
    var ok = file.delete();
    if (!ok) {
      throw new RuntimeException("failed to delete directory");
    }
  }

  /** this is for testing */
  public void deleteDir() {
    var files = dbDirectory.list();
    if (files == null) {
      return;
    }

    for (var f : files) {
      var result = new File(dbDirectory, f).delete();
      if (!result) {
        throw new RuntimeException("failed to delete dir");
      }
    }
    var result = dbDirectory.delete();
    if (!result) {
      throw new RuntimeException("failed to delete dir");
    }
  }
}
