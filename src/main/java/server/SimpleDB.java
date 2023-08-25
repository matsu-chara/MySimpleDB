package server;

import buffer.BufferMgr;
import file.FileMgr;
import index.planner.IndexUpdatePlanner;
import java.io.File;
import log.LogMgr;
import metadata.IndexInfo;
import metadata.MetadataMgr;
import plan.BasicQueryPlanner;
import plan.Planner;
import plan.QueryPlanner;
import plan.UpdatePlanner;
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

  private Planner planner;

  public SimpleDB(String dirname, int blocksize, int buffsize, IndexInfo.IndexMode indexMode) {
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
      mdm = new MetadataMgr(isNew, tx, indexMode);
      QueryPlanner qp = new BasicQueryPlanner(mdm);
      //      UpdatePlanner up = new BasicUpdatePlanner(mdm);
      UpdatePlanner up = new IndexUpdatePlanner(mdm);
      planner = new Planner(qp, up);
      tx.commit();
    }
  }

  public SimpleDB(String dirname, int blocksize, int buffsize) {
    this(dirname, blocksize, buffsize, IndexInfo.IndexMode.Hash);
  }

  public SimpleDB(String dirname) {
    this(dirname, BLOCK_SIZE, BUFFER_SIZE, IndexInfo.IndexMode.Hash);
  }

  public Transaction newTx() {
    return new Transaction(fm, lm, bm);
  }

  public MetadataMgr mdMgr() {
    return mdm;
  }

  public Planner planner() {
    return planner;
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

  public void deleteDB() {
    fm.deleteDir();
  }
}
