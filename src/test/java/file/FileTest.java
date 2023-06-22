package file;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class FileTest {
  private FileMgr fm;
  private String testFileName = "testfile";

  @BeforeEach
  void setup() {
    fm = new FileMgr(new File("test_file"), 400);
  }

  @AfterEach
  void teardown() {
    fm.deleteFile(testFileName);
  }

  @Test
  void writeAndRead() {
    fm.deleteFile(testFileName);

    BlockId blk = new BlockId(testFileName, 2);
    Page p1 = new Page(fm.blockSize());
    int pos1 = 88;
    p1.setString(pos1, "abcdefghijklm");
    int size = Page.maxLength("abcdefghijklm".length());
    int pos2 = pos1 + size;
    p1.setInt(pos2, 345);
    fm.write(blk, p1);

    Page p2 = new Page(fm.blockSize());
    fm.read(blk, p2);
    assertEquals("abcdefghijklm", p2.getString(pos1));
    assertEquals(345, p2.getInt(pos2));
  }
}
