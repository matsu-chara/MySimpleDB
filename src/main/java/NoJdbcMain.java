import java.util.Scanner;
import parser.BadSyntaxException;
import parser.Parser;
import server.SimpleDB;

public class NoJdbcMain {
  public static void main(String[] args) {
    var dirname = (args.length == 0) ? "studentdb" : args[0];
    var db = new SimpleDB(dirname);

    System.out.print("Enter an SQL statement: ");
    var sc = new Scanner(System.in);
    while (sc.hasNext()) {
      var s = sc.nextLine();
      var p = new Parser(s);
      try {
        String result;
        if (s.startsWith("select")) {
          result = p.query().toString();
        } else {
          result = p.updateCmd().getClass().toString();
        }
        System.out.println("Your statement is: " + result);
      } catch (BadSyntaxException ex) {
        System.out.println("Your statement is illegal");
      }
      System.out.print("Enter an SQL statement: ");
    }
    sc.close();
  }
}
