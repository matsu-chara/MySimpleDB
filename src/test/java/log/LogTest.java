package log;

import static org.junit.jupiter.api.Assertions.*;

import file.FileMgr;
import file.Page;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LogTest {
  private FileMgr fm;
  private LogMgr lm;
  String logFilename = "simpledb_log_mgr_test.log";

  @BeforeEach
  void setup() {
    fm = new FileMgr(new File("test_log"), 400);
    lm = new LogMgr(fm, logFilename);
  }

  @AfterEach
  void teardown() {
    fm.deleteFile(logFilename);
  }

  @Test
  void log() {
    createRecords(1, 3);
    var result1 = readLogRecords(); // read時にflushが行われる
    assertIterableEquals(
        List.of(
            new TestLogRecord("record3", 103),
            new TestLogRecord("record2", 102),
            new TestLogRecord("record1", 101)),
        result1);

    createRecords(4, 7);
    var result2 = readLogRecords();
    assertIterableEquals(
        List.of(
            new TestLogRecord("record7", 107),
            new TestLogRecord("record6", 106),
            new TestLogRecord("record5", 105),
            new TestLogRecord("record4", 104),
            new TestLogRecord("record3", 103),
            new TestLogRecord("record2", 102),
            new TestLogRecord("record1", 101)),
        result2);
  }

  private List<TestLogRecord> readLogRecords() {
    var iter = lm.iterator();
    List<TestLogRecord> result = new ArrayList<>();

    while (iter.hasNext()) {
      var rec = iter.next();
      var p = new Page(rec);
      var s = p.getString(0);
      var npos = Page.maxLength(s.length());
      var n = p.getInt(npos);
      result.add(new TestLogRecord(s, n));
    }
    return List.copyOf(result);
  }

  private void createRecords(int start, int end) {
    for (var i = start; i <= end; i++) {
      var rec = createLogRecord(new TestLogRecord("record" + i, i + 100));
      lm.append(rec);
    }
  }

  private byte[] createLogRecord(TestLogRecord r) {
    var npos = Page.maxLength(r.s.length());
    var b = new byte[npos + Integer.BYTES];
    var p = new Page((b));
    p.setString(0, r.s);
    p.setInt(npos, r.n);
    return b;
  }

  static class TestLogRecord {
    String s;
    int n;

    public TestLogRecord(String s, int n) {
      this.s = s;
      this.n = n;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      var that = (TestLogRecord) o;
      return n == that.n && Objects.equals(s, that.s);
    }

    @Override
    public int hashCode() {
      return Objects.hash(s, n);
    }

    @Override
    public String toString() {
      return "TestLogRecord{" + "s='" + s + '\'' + ", n=" + n + '}';
    }
  }
}
