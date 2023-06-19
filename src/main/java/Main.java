import server.SimpleDB;

public class Main {
  public static void main(String[] args) {
    String dirname = (args.length == 0) ? "studentdb" : args[0];
    SimpleDB db = new SimpleDB(dirname);

    System.out.println("Hello world!");
  }
}
