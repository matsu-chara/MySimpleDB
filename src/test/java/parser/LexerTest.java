package parser;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Scanner;
import org.junit.jupiter.api.Test;

class LexerTest {

  @Test
  void test() {
    var sc = new Scanner("abc = 5\n4 = xyz");
    while (sc.hasNext()) {
      var s = sc.nextLine();
      var lex = new Lexer(s);
      String x;
      int y;
      if (lex.matchId()) {
        x = lex.eatId();
        lex.eatDelim('=');
        y = lex.eatIntConstant();
        assertEquals("abc", x);
        assertEquals(5, y);
      } else {
        y = lex.eatIntConstant();
        lex.eatDelim('=');
        x = lex.eatId();
        assertEquals("xyz", x);
        assertEquals(4, y);
      }
    }
    sc.close();
  }
}
