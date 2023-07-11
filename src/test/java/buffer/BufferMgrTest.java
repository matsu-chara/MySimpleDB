package buffer;

import static org.junit.jupiter.api.Assertions.*;

import file.BlockId;
import file.FileMgr;
import java.io.File;
import log.LogMgr;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BufferMgrTest {
  private FileMgr fm;
  private BufferMgr bm;
  String logFilename = "simpledb_buffer_mgr_test.log";

  @BeforeEach
  void setup() {
    fm = new FileMgr(new File("test_buffer_mgr"), 400);
    var lm = new LogMgr(fm, logFilename);
    bm = new BufferMgr(fm, lm, 3, 100 /* ms */);
  }

  @AfterEach
  void teardown() {
    fm.deleteDir();
  }

  @Test
  void buffer() {
    var buff = new Buffer[6];
    buff[0] = bm.pin(new BlockId("testfile", 0));
    buff[1] = bm.pin(new BlockId("testfile", 1));
    buff[2] = bm.pin(new BlockId("testfile", 2));
    bm.unpin(buff[1]);
    buff[1] = null;

    assertEquals(1, bm.available());

    buff[3] = bm.pin(new BlockId("testfile", 0)); // block 0 pinned twice
    buff[4] = bm.pin(new BlockId("testfile", 1)); // block 1 repinned

    assertEquals(0, bm.available());

    assertThrows(
        BufferAbortException.class,
        () -> {
          buff[5] = bm.pin(new BlockId("testfile", 3)); // will not work; no buffers left
        });
    bm.unpin(buff[2]);
    buff[2] = null;

    buff[5] = bm.pin(new BlockId("testfile", 3)); // now this works
  }
}
