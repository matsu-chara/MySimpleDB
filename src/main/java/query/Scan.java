package query;

public interface Scan {
  void beforeFirst();

  boolean next();

  int getInt(String fldname);

  String getString(String fldname);

  Constant getVal(String fldname);

  boolean hasField(String fldname);

  void close();
}
