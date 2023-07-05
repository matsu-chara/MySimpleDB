package buffer;

import static org.junit.jupiter.api.Assertions.*;

import file.BlockId;
import file.FileMgr;
import java.io.File;
import log.LogMgr;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BufferTest {
  private FileMgr fm;
  private BufferMgr bm;
  String logFilename = "simpledb_buffer_test.log";

  @BeforeEach
  void setup() {
    fm = new FileMgr(new File("test_buffer"), 400);
    var lm = new LogMgr(fm, logFilename);
    bm = new BufferMgr(fm, lm, 3);
  }

  @AfterEach
  void teardown() {
    fm.deleteFile(logFilename);
  }

  @Test
  void test() {
    var buff1 = bm.pin(new BlockId("testfile", 1));
    var p = buff1.contents();

    var n = p.getInt(80);
    p.setInt(80, n + 1);
    buff1.setModified(1, 0);
    assertEquals(1, p.getInt(80));
    bm.unpin(buff1);

    // buff sizeが3なのでどこかでbuff1がflushされる
    var buff2 = bm.pin(new BlockId("testfile", 2));
    bm.pin(new BlockId("testfile", 3));
    bm.pin(new BlockId("testfile", 4));

    bm.unpin(buff2);
    buff2 = bm.pin(new BlockId("testfile", 1));
    var p2 = buff2.contents();
    p2.setInt(80, 9999);
    buff2.setModified(1, 0);
    assertEquals(9999, p2.getInt(80));
  }
}
