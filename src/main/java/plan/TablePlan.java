package plan;

import metadata.MetadataMgr;
import metadata.StatInfo;
import query.Scan;
import record.Layout;
import record.Schema;
import record.TableScan;
import tx.Transaction;

public class TablePlan implements Plan {
  private String tblname;
  private Transaction tx;
  private Layout layout;
  private StatInfo si;

  public TablePlan(String tblname, Transaction tx, MetadataMgr md) {
    this.tblname = tblname;
    this.tx = tx;
    this.layout = md.getLayout(tblname, tx);
    si = md.getStatInfo(tblname, layout, tx);
  }

  @Override
  public Scan open() {
    return new TableScan(tx, tblname, layout);
  }

  @Override
  public int blocksAccessed() {
    return si.blocksAccessed();
  }

  @Override
  public int recordsOutput() {
    return si.recordsOutput();
  }

  @Override
  public int distinctValues(String fldname) {
    return si.distinctValues();
  }

  @Override
  public Schema schema() {
    return layout.schema();
  }
}
