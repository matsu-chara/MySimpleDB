package metadata;

import java.util.HashMap;
import java.util.Map;
import record.Layout;
import record.Schema;
import record.TableScan;
import tx.Transaction;

public class TableMgr {
  public static final int MAX_NAME = 16;

  private Layout tcatLayout, fcatLayout; // table catalog, field catalog の略

  public TableMgr(boolean isNew, Transaction tx) {
    var tcatSchema = new Schema();
    tcatSchema.addStringField("tblname", MAX_NAME);
    tcatSchema.addIntField("slotsize");
    this.tcatLayout = new Layout(tcatSchema);

    var fcatSchema = new Schema();
    fcatSchema.addStringField("tblname", MAX_NAME);
    fcatSchema.addStringField("fldname", MAX_NAME);
    fcatSchema.addIntField("type");
    fcatSchema.addIntField("length");
    fcatSchema.addIntField("offset");
    fcatLayout = new Layout(fcatSchema);

    if (isNew) {
      createTable("tblcat", tcatSchema, tx);
      createTable("fldcat", fcatSchema, tx);
    }
  }

  public void createTable(String tblname, Schema sch, Transaction tx) {
    var layout = new Layout(sch);
    var tcat = new TableScan(tx, "tblcat", tcatLayout);

    tcat.insert();
    tcat.setString("tblname", tblname);
    tcat.setInt("slotsize", layout.slotsize());
    tcat.close();

    var fcat = new TableScan(tx, "fldcat", fcatLayout);
    for (var fldname : sch.fields()) {
      fcat.insert();
      fcat.setString("tblname", tblname);
      fcat.setString("fldname", fldname);
      fcat.setInt("type", sch.type(fldname));
      fcat.setInt("length", sch.length(fldname));
      fcat.setInt("offset", layout.offset(fldname));
    }
    fcat.close();
  }

  public Layout getLayout(String tblname, Transaction tx) {
    var size = -1;

    var tcat = new TableScan(tx, "tblcat", tcatLayout);
    while (tcat.next()) {
      if (tcat.getString("tblname").equals(tblname)) {
        size = tcat.getInt("slotsize");
        break;
      }
    }
    tcat.close();

    var sch = new Schema();
    Map<String, Integer> offsets = new HashMap<>();
    var fcat = new TableScan(tx, "fldcat", fcatLayout);
    while (fcat.next()) {
      if (fcat.getString("tblname").equals(tblname)) {
        var fldname = fcat.getString("fldname");
        var fldType = fcat.getInt("type");
        var fldLength = fcat.getInt("length");
        var fldOffset = fcat.getInt("offset");
        offsets.put(fldname, fldOffset);
        sch.addField(fldname, fldType, fldLength);
      }
    }
    fcat.close();
    return new Layout(sch, offsets, size);
  }
}
