import static java.sql.Types.INTEGER;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import jdbc.embedded.EmbeddedDriver;
import jdbc.network.NetworkDriver;
import parser.BadSyntaxException;

public class JdbcMain {
  public static void main(String[] args) {
    var sc = new Scanner(System.in);
    System.out.println("for embedded server:  jdbc:simpledb:studentdb");
    System.out.println("for remote server:  jdbc:simpledb://localhost");
    System.out.println("Connect> ");
    var s = sc.nextLine().trim();
    Driver d = (s.contains("//")) ? new NetworkDriver() : new EmbeddedDriver();

    try (var conn = d.connect(s, null);
        var stmt = conn.createStatement()) {
      initTables(conn);
      insertStudents(conn, 5);
      startInteractiveSqlShell(conn);
    } catch (SQLException e) {
      throw new RuntimeException("failed to run", e);
    }
  }

  private static void initTables(Connection conn) throws SQLException {
    var stmt = conn.createStatement();
    var cmd = "CREATE TABLE student(sid int, sname varchar(12))";
    stmt.executeUpdate(cmd);
  }

  private static void insertStudents(Connection conn, int n) throws SQLException {
    var stmt = conn.createStatement();
    var delCmd = "DELETE FROM student";
    stmt.executeUpdate(delCmd);

    var rand = new Random();
    for (var i = 0; i < n; i++) {
      var v = rand.nextInt(10000);
      var cmd = "INSERT INTO student(sid, sname) VALUES (" + v + ", 'tanaka" + v + "')";
      stmt.executeUpdate(cmd);
    }
  }

  private static void startInteractiveSqlShell(Connection conn) throws SQLException {
    var stmt = conn.createStatement();

    System.out.print("Enter an SQL statement> ");
    var sc = new Scanner(System.in);
    while (sc.hasNext()) {
      var sql = sc.nextLine().trim(); // select sid, sname from student がおすすめ
      try {
        if (sql.startsWith("exit")) {
          break;
        } else if (sql.startsWith("select")) {
          var rs = stmt.executeQuery(sql);
          print(rs);
        } else {
          var result = stmt.executeUpdate(sql);
          System.out.println(result + " records processed.");
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

    var meta = rs.getMetaData();
    var columnum = meta.getColumnCount();
    var rowlength = 0;

    // header
    for (var i = 1; i <= columnum; i++) {
      var digit = meta.getColumnDisplaySize(i);
      var format = "%" + digit + "s";
      header.add(String.format(format, meta.getColumnName(i)));
      rowlength += digit + 1; // delimiterの分も追加
    }
    System.out.println("+" + "-".repeat(rowlength - 1) + "+");
    System.out.println("|" + String.join("|", header) + "|");

    System.out.println("+" + "-".repeat(rowlength - 1) + "+");

    while (rs.next()) {
      List<String> body = new ArrayList<>();

      for (var i = 1; i <= columnum; i++) {
        var digit = meta.getColumnDisplaySize(i);
        var format = "%" + digit + "s";

        var columnname = meta.getColumnName(i);
        var tpe = meta.getColumnType(i);
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
