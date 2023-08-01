package parser;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import org.junit.jupiter.api.Test;
import query.Constant;

class ParserTest {
  @Test
  void testSelect() {
    var s = "select A, B from T1, T2 where A=1 and B=x";
    var p = new Parser(s);
    assertEquals(s.toLowerCase(), p.query().toString());
  }

  @Test
  void testInsert() {
    var s = "insert into A (a,b,c) values (1, 2, 3)";
    var p = new Parser(s);

    var result = (InsertData) p.updateCmd();
    assertEquals("a", result.tblname());
    assertEquals(Arrays.asList("a", "b", "c"), result.fields());
    assertEquals(Arrays.asList(new Constant(1), new Constant(2), new Constant(3)), result.vals());
  }

  @Test
  void testUpdate() {
    var s = "update T set B = 'a' where B=x and A=1";
    var p = new Parser(s);

    var result = (ModifyData) p.updateCmd();
    assertEquals("t", result.tblname());
    assertEquals("b", result.targetField());
    assertEquals("a", result.newval().toString());
    assertEquals("b=x and a=1", result.pred().toString());
  }

  @Test
  void testDelete() {
    var s = "delete from T where A=1 and B=x";
    var p = new Parser(s);
    var result = (DeleteData) p.updateCmd();
    assertEquals("t", result.tblname());
    assertEquals("a=1 and b=x", result.pred().toString());
  }
}
