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

  public int blocksAccessed() {
    return si.blocksAccessed();
  }

  public int recordsOutput() {
    return si.recordsOutput();
  }

  public int distinctValues() {
    return si.distinctValues();
  }

  public Schema schema() {
    return layout.schema();
  }
}
