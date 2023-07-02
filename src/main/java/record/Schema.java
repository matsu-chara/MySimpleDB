package record;

import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Schema {
  private List<String> fields = new ArrayList<>();
  private Map<String, FieldInfo> info = new HashMap<>();

  public void addField(String fldname, int type, int length) {
    fields.add(fldname);
    info.put(fldname, new FieldInfo(type, length));
  }

  public void addIntField(String fildname) {
    addField(fildname, INTEGER, 0); // lengthフィールドはtype=INTEGERではアクセスされないのでダミー値を入れておく
  }

  public void addStringField(String fildname, int length) {
    addField(fildname, VARCHAR, length);
  }

  public void add(String fldname, Schema sch) {
    int type = sch.type(fldname);
    int length = sch.length(fldname);
    addField(fldname, type, length);
  }

  public void addAll(Schema sch) {
    for (String fldname : sch.fields()) {
      add(fldname, sch);
    }
  }

  public List<String> fields() {
    return fields;
  }

  public boolean hasField(String fldname) {
    return fields.contains(fldname);
  }

  public int type(String fldname) {
    return info.get(fldname).type;
  }

  public int length(String fldname) {
    return info.get(fldname).length;
  }

  class FieldInfo {
    int type, length;

    public FieldInfo(int type, int length) {
      this.type = type;
      this.length = length;
    }
  }
}
