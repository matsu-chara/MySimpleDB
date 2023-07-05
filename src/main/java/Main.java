import server.SimpleDB;

public class Main {
  public static void main(String[] args) {
    var dirname = (args.length == 0) ? "studentdb" : args[0];
    var db = new SimpleDB(dirname);

    System.out.println("Hello world!");
  }
}
