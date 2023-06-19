package server;

import file.FileMgr;
import java.io.File;

public class SimpleDB {
  public static int BLOCK_SIZE = 400;
  public static int BUFFER_SIZE = 8;

  private FileMgr fm;

  public SimpleDB(String dirname, int blocksize, int buffsize) {
    File dbDirectory = new File(dirname);
    fm = new FileMgr(dbDirectory, blocksize);
  }

  public SimpleDB(String dirname) {
    this(dirname, BLOCK_SIZE, BUFFER_SIZE);
  }
}
