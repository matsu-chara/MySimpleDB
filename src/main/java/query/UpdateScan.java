package query;

import record.RID;

public interface UpdateScan extends Scan {
  void setVal(String fldname, Constant val);

  void setInt(String fldname, int val);

  void setString(String fldname, String val);

  void insert();

  void delete();

  RID getRid();

  void moveToRid(RID rid);
}
