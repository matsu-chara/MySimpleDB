package buffer;

import file.BlockId;
import file.FileMgr;
import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

import file.Page;
import log.LogMgr;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BufferFileTest {
  private FileMgr fm;
  private LogMgr lm;
  private BufferMgr bm;
  String logFilename = "simpledb_buffer_file_test.log";

  @BeforeEach
  void setup() {
    fm = new FileMgr(new File("test_buffer_file"), 400);
    lm = new LogMgr(fm, logFilename);
    bm = new BufferMgr(fm, lm, 8);
  }

  @AfterEach
  void teardown() {
    fm.deleteFile(logFilename);
  }

  @Test
  void test() {
    BlockId blk = new BlockId("testfile", 2);
    int pos1 = 88;

    Buffer b1 = bm.pin(blk);
    Page   p1 = b1.contents();
    p1.setString(pos1, "abcdefghijklm");
    int size = Page.maxLength("abcdefghijklm".length());

    int pos2 = pos1 + size;
    p1.setInt(pos2, 345);
    b1.setModified(1, 0);
    bm.unpin(b1);

    Buffer b2 = bm.pin(blk);
    Page p2 = b2.contents();
    assertEquals(345, p2.getInt(pos2));
    assertEquals("abcdefghijklm",p2.getString(pos1));
    bm.unpin(b2);
  }
}
