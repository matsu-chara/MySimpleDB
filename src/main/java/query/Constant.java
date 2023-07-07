package query;

public class Constant implements Comparable<Constant> {
  private Integer ival = null;
  private String sval = null;

  public Constant(Integer ival) {
    this.ival = ival;
  }

  public Constant(String sval) {
    this.sval = sval;
  }

  public int asInt() {
    return ival;
  }

  public String asString() {
    return sval;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    var constant = (Constant) o;
    return (ival != null) ? ival.equals(constant.ival) : sval.equals(constant.sval);
  }

  @Override
  public int compareTo(Constant c) {
    return (ival != null) ? ival.compareTo(c.ival) : sval.compareTo(c.sval);
  }

  @Override
  public int hashCode() {
    return (ival != null) ? ival.hashCode() : sval.hashCode();
  }

  @Override
  public String toString() {
    return (ival != null) ? ival.toString() : sval;
  }
}
