package query;

import record.Schema;

public class Expression {
  private Constant val = null;
  private String fldname = null;

  public Expression(Constant val) {
    this.val = val;
  }

  public Expression(String fldname) {
    this.fldname = fldname;
  }

  public Constant evaluate(Scan s) {
    return (val != null) ? val : s.getVal(fldname);
  }

  public boolean isFieldName() {
    return fldname != null;
  }

  public Constant asConstant() {
    return val;
  }

  public String asFieldName() {
    return fldname;
  }

  public boolean appliesTo(Schema sch) {
    return val != null || sch.hasField(fldname);
  }

  public String toString() {
    return (val != null) ? val.toString() : fldname;
  }
}
