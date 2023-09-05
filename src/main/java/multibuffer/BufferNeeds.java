package multibuffer;

public class BufferNeeds {
  public static int bestRoot(int available, int size) {
    var avail = available - 2;
    if (avail <= 1) {
      return 1;
    }
    var k = Integer.MAX_VALUE;
    var i = 1.0;
    while (k > avail) {
      i++;
      k = (int) Math.ceil(Math.pow(size, 1 / i));
    }
    return k;
  }

  public static int bestFactor(int available, int size) {
    var avail = available - 2;
    if (avail <= 1) {
      return 1;
    }
    var k = size;
    var i = 1.0;
    while (k > avail) {
      i++;
      k = (int) Math.ceil(size / i);
    }
    return k;
  }
}
