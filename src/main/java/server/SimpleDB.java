package server;

import buffer.BufferMgr;
import file.FileMgr;
import java.io.File;
import log.LogMgr;
import tx.Transaction;

public class SimpleDB {
  public static int BLOCK_SIZE = 400;
  public static int BUFFER_SIZE = 8;

  public static String LOG_FILE = "simpledb.log";

  private FileMgr fm;
  private LogMgr lm;

  private BufferMgr bm;

  public SimpleDB(String dirname, int blocksize, int buffsize) {
    var dbDirectory = new File(dirname);
    fm = new FileMgr(dbDirectory, blocksize);
    lm = new LogMgr(fm, LOG_FILE);
    bm = new BufferMgr(fm, lm, buffsize);
  }

  public SimpleDB(String dirname) {
    this(dirname, BLOCK_SIZE, BUFFER_SIZE);
  }

  public Transaction newTx() {
    return new Transaction(fm, lm, bm);
  }

  public FileMgr fileMgr() {
    return fm;
  }

  public LogMgr logMgr() {
    return lm;
  }

  public BufferMgr bufferMgr() {
    return bm;
  }
}
