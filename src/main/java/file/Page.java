package file;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/** ブロックと同じサイズのメモリ領域 blockをPageに読み込み、Pageを書き換えたあと、Pageをblockに書き戻すことでデータが変わる */
public class Page {
  private ByteBuffer bb;
  public static final Charset CHARSET = StandardCharsets.US_ASCII;

  // データバッファー用
  public Page(int blocksize) {
    this.bb = ByteBuffer.allocateDirect(blocksize);
  }

  // log page用
  public Page(byte[] b) {
    this.bb = ByteBuffer.wrap(b);
  }

  public int getInt(int offset) {
    return bb.getInt(offset);
  }

  public void setInt(int offset, int n) {
    bb.putInt(offset, n);
  }

  public byte[] getBytes(int offset) {
    bb.position(offset);

    // length + 本体 の順番に並んでいる
    var length = bb.getInt();
    var b = new byte[length];
    bb.get(b);
    return b;
  }

  public void setBytes(int offset, byte[] b) {
    // length + 本体 の順番に書き込む
    bb.position(offset);
    bb.putInt(b.length);
    bb.put(b);
  }

  public String getString(int offset) {
    var b = getBytes(offset);
    return new String(b, CHARSET);
  }

  public void setString(int offset, String s) {
    var b = s.getBytes(CHARSET);
    setBytes(offset, b);
  }

  public static int maxLength(int strlen) {
    var bytesPerChar = CHARSET.newEncoder().maxBytesPerChar();
    return Integer.BYTES + (strlen * (int) bytesPerChar);
  }

  // FileMgr用にpackage private
  ByteBuffer contents() {
    bb.position(0);
    return bb;
  }
}
