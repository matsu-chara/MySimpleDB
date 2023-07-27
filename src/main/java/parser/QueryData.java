package parser;

import java.util.Collection;
import java.util.List;
import query.Predicate;

public class QueryData {
  private List<String> fields;
  private Collection<String> tables;
  private Predicate pred;

  public QueryData(List<String> fields, Collection<String> tables, Predicate pred) {
    this.fields = fields;
    this.tables = tables;
    this.pred = pred;
  }

  public List<String> fields() {
    return fields;
  }

  public Collection<String> tables() {
    return tables;
  }

  public Predicate pred() {
    return pred;
  }

  public String toString() {
    var result = "select ";
    result += String.join(", ", fields);
    result += " from ";
    result += String.join(", ", tables);
    var predstring = pred.toString();
    if (!predstring.isEmpty()) result += " where " + predstring;
    return result;
  }
}
