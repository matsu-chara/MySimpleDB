package parser;

public class CreateIndexData {
  private String idxname, tblname, fldname;

  public CreateIndexData(String idxname, String tblname, String fldname) {
    this.idxname = idxname;
    this.tblname = tblname;
    this.fldname = fldname;
  }

  public String idxname() {
    return idxname;
  }

  public String tblname() {
    return tblname;
  }

  public String fldname() {
    return fldname;
  }
}
