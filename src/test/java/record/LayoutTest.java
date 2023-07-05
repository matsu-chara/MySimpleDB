package record;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class LayoutTest {
  @Test
  void test() {
    var sch = new Schema();
    sch.addIntField("a");
    sch.addStringField("b", 9);
    var layout = new Layout(sch);

    assertEquals(Integer.BYTES, layout.offsets("a"));
    assertEquals(Integer.BYTES * 2, layout.offsets("b"));
  }
}
