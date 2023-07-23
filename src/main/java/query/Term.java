package query;

import record.Schema;

public class Term {
  private Expression lhs, rhs;

  public Term(Expression lhs, Expression rhs) {
    this.lhs = lhs;
    this.rhs = rhs;
  }

  public boolean isSatisfied(Scan s) {
    var lhsval = lhs.evaluate(s);
    var rhsval = rhs.evaluate(s);
    return rhsval.equals(lhsval);
  }

  public int reductionFactor(Plan p) {
    String lhsname, rhsname;
    if (lhs.isFieldName() && rhs.isFieldName()) {
      lhsname = lhs.asFieldName();
      rhsname = rhs.asFieldName();
      return Math.max(p.distinctValues(lhsname), p.distinctValues(rhsname));
    }
    if (lhs.isFieldName()) {
      lhsname = lhs.asFieldName();
      return p.distinctValues(lhsname);
    }
    if (rhs.isFieldName()) {
      rhsname = rhs.asFieldName();
      return p.distinctValues(rhsname);
    }

    if (lhs.asConstant().equals(rhs.asConstant())) {
      return 1;
    } else {
      return Integer.MAX_VALUE;
    }
  }

  public Constant equatesWithConstant(String fldname) {
    if (lhs.isFieldName() && lhs.asFieldName().equals(fldname) && !rhs.isFieldName()) {
      return rhs.asConstant();
    } else if (rhs.isFieldName() && rhs.asFieldName().equals(fldname) && !lhs.isFieldName()) {
      return lhs.asConstant();
    } else {
      return null;
    }
  }

  public String equatesWithField(String fldname) {
    if (lhs.isFieldName() && lhs.asFieldName().equals(fldname) && rhs.isFieldName()) {
      return rhs.asFieldName();
    } else if (rhs.isFieldName() && rhs.asFieldName().equals(fldname) && lhs.isFieldName()) {
      return lhs.asFieldName();
    } else {
      return null;
    }
  }

  public boolean appliesTo(Schema sch) {
    return lhs.appliesTo(sch) && rhs.appliesTo(sch);
  }

  public String toString() {
    return lhs.toString() + "=" + rhs.toString();
  }
}
