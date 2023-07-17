package index;

import query.Constant;
import record.RID;

public interface Index {
  public void beforeFirst(Constant searchkey);

  public void next();

  public RID getRid();

  public void insert(Constant dataval, RID datarid);

  public void delete(Constant dataval, RID datarid);

  public void close();
}
