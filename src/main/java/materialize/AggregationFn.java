package materialize;

import query.Constant;
import query.Scan;

public interface AggregationFn {
  void processFirst(Scan s);

  void processNext(Scan s);

  String fieldName();

  Constant value();
}
