package buffer;

import file.FileMgr;
import log.LogMgr;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

class BufferTest {
    private FileMgr   fm;
    private     LogMgr    lm;
    private     BufferMgr bm;
    String logFilename = "simpledb_buffer_test.log";

    @BeforeEach
    void setup() {
        fm = new FileMgr(new File("test_buffer"), 400);
        lm = new LogMgr(fm, logFilename);
        bm = new BufferMgr(fm, lm, 3, 100 /* ms */);
    }

    @AfterEach
    void teardown() {
        fm.deleteFile(logFilename);
    }

        @Test
    void test() {

    }
}
