package server;

import buffer.BufferMgr;
import file.FileMgr;
import java.io.File;
import log.LogMgr;
import metadata.MetadataMgr;
import tx.Transaction;

public class SimpleDB {
  public static int BLOCK_SIZE = 400;
  public static int BUFFER_SIZE = 8;

  public static String LOG_FILE = "simpledb.log";

  private FileMgr fm;
  private BufferMgr bm;
  private LogMgr lm;
  private MetadataMgr mdm;
  private static final Object lock = new Object();

  public SimpleDB(String dirname, int blocksize, int buffsize) {
    var dbDirectory = new File(dirname);

    synchronized (lock) {
      fm = new FileMgr(dbDirectory, blocksize);
      lm = new LogMgr(fm, LOG_FILE);
      bm = new BufferMgr(fm, lm, buffsize);

      var tx = newTx();
      var isNew = fm.isNew();
      if (isNew) {
        System.out.println("creating new database");
      } else {
        System.out.println("recovering existing database");
        tx.recover();
      }
      mdm = new MetadataMgr(isNew, tx);
      tx.commit();
    }
  }

  public SimpleDB(String dirname) {
    this(dirname, BLOCK_SIZE, BUFFER_SIZE);
  }

  public Transaction newTx() {
    return new Transaction(fm, lm, bm);
  }

  public MetadataMgr mdMgr() {
    return mdm;
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
