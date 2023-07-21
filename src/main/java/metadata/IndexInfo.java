package metadata;

import static java.sql.Types.INTEGER;

import index.Index;
import record.Layout;
import record.Schema;
import tx.Transaction;

public class IndexInfo {
  private String idxname, fldname;
  private Transaction tx;
  private Schema tblSchema;
  private Layout idxLayout;
  private StatInfo si;

  public IndexInfo(String idxname, String fldname, Transaction tx, Schema tblSchema, StatInfo si) {
    this.idxname = idxname;
    this.fldname = fldname;
    this.tx = tx;
    this.tblSchema = tblSchema;
    this.idxLayout = createIdxLayout();
    this.si = si;
  }

  public Index open() {
    return null; // TODO
  }

  public int blocksAccessed() {
    var rpb = tx.blockSize() / idxLayout.slotsize();
    var numblocks = si.recordsOutput() / rpb;
    return -1; // TODO
  }

  public int recordsOutput() {
    return si.recordsOutput() / si.distinctValues();
  }

  public int distinctValues(String fname) {
    return fldname.equals(fname) ? 1 : si.distinctValues();
  }

  private Layout createIdxLayout() {
    var sch = new Schema();
    sch.addIntField("block");
    sch.addIntField("id");
    if (tblSchema.type(fldname) == INTEGER) {
      sch.addIntField("dataval");
    } else {
      var fldlen = tblSchema.length(fldname);
      sch.addStringField("dataval", fldlen);
    }
    return new Layout(sch);
  }
}
