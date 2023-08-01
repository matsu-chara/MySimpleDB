package query;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import plan.Plan;
import record.Schema;

public class Predicate {
  private List<Term> terms = new ArrayList<>();

  public Predicate() {}

  public Predicate(Term term) {
    terms.add(term);
  }

  public void conjoinWith(Predicate pred) {
    terms.addAll(pred.terms);
  }

  public boolean isSatisfied(Scan s) {
    return terms.stream().allMatch(t -> t.isSatisfied(s));
  }

  public int reductionFactor(Plan p) {
    return terms.stream().mapToInt(t -> t.reductionFactor(p)).reduce(1, (a, b) -> a * b);
  }

  public Predicate selectSubPred(Schema sch) {
    var result = new Predicate();
    for (var t : terms) {
      if (t.appliesTo(sch)) {
        result.terms.add(t);
      }
    }
    if (result.terms.isEmpty()) {
      return null;
    } else {
      return result;
    }
  }

  public Predicate joinSubPred(Schema sch1, Schema sch2) {
    var result = new Predicate();
    var newsch = new Schema();
    newsch.addAll(sch1);
    newsch.addAll(sch2);
    for (var t : terms) {
      if (!t.appliesTo(sch1) && !t.appliesTo(sch2) && t.appliesTo(newsch)) {
        result.terms.add(t);
      }
    }
    if (result.terms.isEmpty()) {
      return null;
    } else {
      return result;
    }
  }

  public Constant equatesWithConstant(String fldname) {
    return terms.stream()
        .map(t -> t.equatesWithConstant(fldname))
        .filter(Objects::nonNull)
        .findFirst()
        .orElse(null);
  }

  public String equatesWithField(String fldname) {
    return terms.stream()
        .map(t -> t.equatesWithField(fldname))
        .filter(Objects::nonNull)
        .findFirst()
        .orElse(null);
  }

  public String toString() {
    var iter = terms.iterator();
    if (!iter.hasNext()) {
      return "";
    }

    var result = new StringBuilder(iter.next().toString());
    while (iter.hasNext()) {
      result.append(" and ").append(iter.next().toString());
    }
    return result.toString();
  }
}
