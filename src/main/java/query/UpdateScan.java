package query;

import record.RID;

public interface UpdateScan extends Scan {
  public void setVal(String fldname, Constant val);

  public void setInt(String fldname, int val);

  public void setString(String fldname, String val);

  public void insert();

  public void delete();

  public RID getRid();

  public void moveToRid(RID rid);
}
