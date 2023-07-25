package index;

import query.Constant;
import record.RID;

public interface Index {
  void beforeFirst(Constant searchkey);

  void next();

  RID getRid();

  void insert(Constant dataval, RID datarid);

  void delete(Constant dataval, RID datarid);

  void close();
}
