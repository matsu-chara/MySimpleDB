package parser;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import query.Constant;
import query.Expression;
import query.Predicate;
import query.Term;
import record.Schema;

public class Parser {
  private Lexer lex;

  public Parser(String s) {
    lex = new Lexer(s);
  }

  public String field() {
    return lex.eatId();
  }

  public Constant constant() {
    if (lex.matchStringConstant()) {
      return new Constant(lex.eatStringConstant());
    } else {
      return new Constant(lex.eatIntConstant());
    }
  }

  public Expression expression() {
    if (lex.matchId()) {
      return new Expression(field());
    } else {
      return new Expression(constant());
    }
  }

  public Term term() {
    var lhs = expression();
    lex.eatDelim('=');
    var rhs = expression();
    return new Term(lhs, rhs);
  }

  public Predicate predicate() {
    var pred = new Predicate(term());
    if (lex.matchKeyword("and")) {
      lex.eatKeyword("and");
      pred.conjoinWith(predicate());
    }
    return pred;
  }

  public QueryData query() {
    lex.eatKeyword("select");
    var fields = selectList();
    lex.eatKeyword("from");
    var tables = tableList();
    var pred = new Predicate();
    if (lex.matchKeyword("where")) {
      lex.eatKeyword("where");
      pred = predicate();
    }
    return new QueryData(fields, tables, pred);
  }

  private List<String> selectList() {
    List<String> L = new ArrayList<>();
    L.add(field());
    if (lex.matchDelim(',')) {
      lex.eatDelim(',');
      L.addAll(selectList());
    }
    return L;
  }

  private Collection<String> tableList() {
    Collection<String> L = new ArrayList<>();
    L.add(lex.eatId());
    if (lex.matchDelim(',')) {
      lex.eatDelim(',');
      L.addAll(tableList());
    }
    return L;
  }

  public Object updateCmd() {
    if (lex.matchKeyword("insert")) {
      return insert();
    } else if (lex.matchKeyword("delete")) {
      return delete();
    } else if (lex.matchKeyword("update")) {
      return modify();
    } else {
      return create();
    }
  }

  private Object create() {
    lex.eatKeyword("create");
    if (lex.matchKeyword("table")) {
      return createTable();
    } else if (lex.matchKeyword("view")) {
      return createView();
    } else {
      return createIndex();
    }
  }

  public DeleteData delete() {
    lex.eatKeyword("delete");
    lex.eatKeyword("from");
    var tblname = lex.eatId();
    var pred = new Predicate();
    if (lex.matchKeyword("where")) {
      lex.eatKeyword("where");
      pred = predicate();
    }
    return new DeleteData(tblname, pred);
  }

  public InsertData insert() {
    lex.eatKeyword("insert");
    lex.eatKeyword("into");
    var tblname = lex.eatId();
    lex.eatDelim('(');
    var fields = fieldList();
    lex.eatDelim(')');
    lex.eatKeyword("values");
    lex.eatDelim('(');
    var consts = constList();
    lex.eatDelim(')');
    return new InsertData(tblname, fields, consts);
  }

  private List<String> fieldList() {
    List<String> L = new ArrayList<>();
    L.add(field());
    if (lex.matchDelim(',')) {
      lex.eatDelim(',');
      L.addAll(fieldList());
    }
    return L;
  }

  private List<Constant> constList() {
    List<Constant> L = new ArrayList<>();
    L.add(constant());
    if (lex.matchDelim(',')) {
      lex.eatDelim(',');
      L.addAll(constList());
    }
    return L;
  }

  public ModifyData modify() {
    lex.eatKeyword("update");
    var tblname = lex.eatId();
    lex.eatKeyword("set");
    var fldname = field();
    lex.eatDelim('=');
    var newval = expression();
    var pred = new Predicate();
    if (lex.matchKeyword("where")) {
      lex.eatKeyword("where");
      pred = predicate();
    }
    return new ModifyData(tblname, fldname, newval, pred);
  }

  public CreateTableData createTable() {
    lex.eatKeyword("table");
    var tblname = lex.eatId();
    lex.eatDelim('(');
    var sch = fieldDefs();
    lex.eatDelim(')');
    return new CreateTableData(tblname, sch);
  }

  private Schema fieldDefs() {
    var schema = fieldDef();
    if (lex.matchDelim(',')) {
      lex.eatDelim(',');
      var sch2 = fieldDefs();
      schema.addAll(sch2);
    }
    return schema;
  }

  private Schema fieldDef() {
    var fldname = field();
    return fieldType(fldname);
  }

  private Schema fieldType(String fldname) {
    var schema = new Schema();
    if (lex.matchKeyword("int")) {
      lex.eatKeyword("int");
      schema.addIntField(fldname);
    } else {
      lex.eatKeyword("varchar");
      lex.eatDelim('(');
      var strLen = lex.eatIntConstant();
      lex.eatDelim(')');
      schema.addStringField(fldname, strLen);
    }
    return schema;
  }

  public CreateViewData createView() {
    lex.eatKeyword("view");
    var viewname = lex.eatId();
    lex.eatKeyword("as");
    var qd = query();
    return new CreateViewData(viewname, qd);
  }

  public CreateIndexData createIndex() {
    lex.eatKeyword("index");
    var idxname = lex.eatId();
    lex.eatKeyword("on");
    var tblname = lex.eatId();
    lex.eatDelim('(');
    var fldname = field();
    lex.eatDelim(')');
    return new CreateIndexData(idxname, tblname, fldname);
  }
}
