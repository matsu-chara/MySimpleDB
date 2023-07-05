package buffer;

import static org.junit.jupiter.api.Assertions.*;

import file.BlockId;
import file.FileMgr;
import file.Page;
import java.io.File;
import log.LogMgr;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BufferFileTest {
  private FileMgr fm;
  private BufferMgr bm;
  String logFilename = "simpledb_buffer_file_test.log";

  @BeforeEach
  void setup() {
    fm = new FileMgr(new File("test_buffer_file"), 400);
    var lm = new LogMgr(fm, logFilename);
    bm = new BufferMgr(fm, lm, 8);
  }

  @AfterEach
  void teardown() {
    fm.deleteFile(logFilename);
  }

  @Test
  void test() {
    var blk = new BlockId("testfile", 2);
    var pos1 = 88;

    var b1 = bm.pin(blk);
    var p1 = b1.contents();
    p1.setString(pos1, "abcdefghijklm");
    var size = Page.maxLength("abcdefghijklm".length());

    var pos2 = pos1 + size;
    p1.setInt(pos2, 345);
    b1.setModified(1, 0);
    bm.unpin(b1);

    var b2 = bm.pin(blk);
    var p2 = b2.contents();
    assertEquals(345, p2.getInt(pos2));
    assertEquals("abcdefghijklm", p2.getString(pos1));
    bm.unpin(b2);
  }
}
