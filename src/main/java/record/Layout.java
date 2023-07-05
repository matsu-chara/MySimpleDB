package record;

import static java.sql.Types.INTEGER;

import file.Page;
import java.util.HashMap;
import java.util.Map;

public class Layout {
  private Schema schema;
  private Map<String, Integer> offsets;
  private int slotsize;

  public Layout(Schema schema) {
    this.schema = schema;
    offsets = new HashMap<>();
    var pos = Integer.BYTES; // leave space for the empty/inuse flag
    for (var fldname : schema.fields()) {
      offsets.put(fldname, pos);
      pos += lengthInBytes(fldname);
    }
    slotsize = pos;
  }

  /** catalog からLayoutを取得したときに利用する */
  public Layout(Schema schema, Map<String, Integer> offsets, int slotsize) {
    this.schema = schema;
    this.offsets = offsets;
    this.slotsize = slotsize;
  }

  public Schema schema() {
    return schema;
  }

  public Integer offsets(String fldname) {
    return offsets.get(fldname);
  }

  public int slotsize() {
    return slotsize;
  }

  private int lengthInBytes(String fldname) {
    var fldtype = schema.type(fldname);
    if (fldtype == INTEGER) {
      return Integer.BYTES;
    } else {
      // fldtype == VARCHAR
      return Page.maxLength(schema.length(fldname));
    }
  }
}
