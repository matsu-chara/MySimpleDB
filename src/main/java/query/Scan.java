package query;

public interface Scan {
  public void beforeFirst();

  public boolean next();

  public int getInt();

  public String getString();

  public Constant getVal();

  public boolean hasField(String fldname);

  public void close();
}
