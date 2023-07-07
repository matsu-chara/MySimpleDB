package query;

public interface Scan {
  void beforeFirst();

  boolean next();

  int getInt();

  String getString();

  Constant getVal();

  boolean hasField(String fldname);

  void close();
}
