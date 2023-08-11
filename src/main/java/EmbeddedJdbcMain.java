import static java.sql.Types.INTEGER;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import jdbc.embedded.EmbeddedDriver;
import parser.BadSyntaxException;

public class EmbeddedJdbcMain {
  public static void main(String[] args) {
    String dirname = (args.length == 0) ? "studentdb" : args[0];
    String url = "jdbc:simpledb:" + dirname;

    Driver d = new EmbeddedDriver();
    try (Connection conn = d.connect(url, null)) {
      initTables(conn);
      insertStudents(conn, 5);
      startInteractiveSqlShell(conn);
    } catch (SQLException e) {
      throw new RuntimeException("failed to run", e);
    }
  }

  private static void initTables(Connection conn) throws SQLException {
    Statement stmt = conn.createStatement();
    String cmd = "CREATE TABLE student(sid int, sname varchar(12))";
    stmt.executeUpdate(cmd);
  }

  private static void insertStudents(Connection conn, int n) throws SQLException {
    Statement stmt = conn.createStatement();
    String delCmd = "DELETE FROM student";
    stmt.executeUpdate(delCmd);

    Random rand = new Random();
    for (int i = 0; i < n; i++) {
      int v = rand.nextInt(10000);
      String cmd = "INSERT INTO student(sid, sname) VALUES (" + v + ", 'tanaka" + v + "')";
      stmt.executeUpdate(cmd);
    }
  }

  private static void startInteractiveSqlShell(Connection conn) throws SQLException {
    Statement stmt = conn.createStatement();

    System.out.print("Enter an SQL statement> ");
    Scanner sc = new Scanner(System.in);
    while (sc.hasNext()) {
      String sql = sc.nextLine(); // select sid, sname from student がおすすめ
      try {
        if (sql.startsWith("select")) {
          ResultSet rs = stmt.executeQuery(sql);
          print(rs);
        } else {
          stmt.executeUpdate(sql);
        }
      } catch (SQLException e) {
        if (e.getCause() instanceof BadSyntaxException) {
          System.out.println("unable to parse " + sql + ". please try again.");
        } else {
          throw e;
        }
      }
      System.out.print("Enter an SQL statement> ");
    }
    sc.close();
  }

  private static void print(ResultSet rs) throws SQLException {
    List<String> header = new ArrayList<>();

    ResultSetMetaData meta = rs.getMetaData();
    int columnum = meta.getColumnCount();
    int rowlength = 0;

    // header
    for (int i = 1; i <= columnum; i++) {
      int digit = meta.getColumnDisplaySize(i);
      String format = "%" + digit + "s";
      header.add(String.format(format, meta.getColumnName(i)));
      rowlength += digit + 1; // delimiterの分も追加
    }
    System.out.println("+" + "-".repeat(rowlength - 1) + "+");
    System.out.println("|" + String.join("|", header) + "|");

    System.out.println("+" + "-".repeat(rowlength - 1) + "+");

    while (rs.next()) {
      List<String> body = new ArrayList<>();

      for (int i = 1; i <= columnum; i++) {
        int digit = meta.getColumnDisplaySize(i);
        String format = "%" + digit + "s";

        String columnname = meta.getColumnName(i);
        int tpe = meta.getColumnType(i);
        if (tpe == INTEGER) {
          body.add(String.format(format, rs.getInt(columnname)));
        } else {
          body.add(String.format(format, rs.getString(columnname)));
        }
      }
      System.out.println("|" + String.join("|", body) + "|");
    }
    System.out.println("+" + "-".repeat(rowlength - 1) + "+");
  }
}
