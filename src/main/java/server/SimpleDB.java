package server;

import file.FileMgr;
import log.LogMgr;

import java.io.File;

public class SimpleDB {
  public static int BLOCK_SIZE = 400;
  public static int BUFFER_SIZE = 8;

  public static String LOG_FILE = "simpledb.log";

  private FileMgr fm;
  private LogMgr lm;

  public SimpleDB(String dirname, int blocksize, int buffsize) {
    File dbDirectory = new File(dirname);
    fm = new FileMgr(dbDirectory, blocksize);
    lm = new LogMgr(fm, LOG_FILE);
  }

  public SimpleDB(String dirname) {
    this(dirname, BLOCK_SIZE, BUFFER_SIZE);
  }
}
