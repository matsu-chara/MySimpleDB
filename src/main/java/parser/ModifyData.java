package parser;

import query.Expression;
import query.Predicate;

public class ModifyData {
  private String tblname;
  private String fldname;
  private Expression newval;
  private Predicate pred;

  public ModifyData(String tblname, String fldname, Expression newval, Predicate pred) {
    this.tblname = tblname;
    this.fldname = fldname;
    this.newval = newval;
    this.pred = pred;
  }

  public String tblname() {
    return tblname;
  }

  public String fieldname() {
    return fldname;
  }

  public Expression newval() {
    return newval;
  }

  public Predicate pred() {
    return pred;
  }
}
