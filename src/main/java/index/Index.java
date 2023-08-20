package index;

import query.Constant;
import record.RID;

public interface Index {
  public void beforeFirst(Constant searchkey);

  public boolean next();

  public RID getDataRid();

  public void insert(Constant dataval, RID datarid);

  public void delete(Constant dataval, RID datarid);

  public void close();
}
