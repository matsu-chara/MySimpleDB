package parser;

public class CreateViewData {
  private String viewname;
  private QueryData qrydata;

  public CreateViewData(String viewname, QueryData qrydata) {
    this.viewname = viewname;
    this.qrydata = qrydata;
  }

  public String viewname() {
    return viewname;
  }

  public String viewDef() {
    return qrydata.toString();
  }
}
