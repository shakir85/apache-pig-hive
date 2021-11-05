package logsparser;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Hive_Java {

    private static String driver = "org.apache.hive.jdbc.HiveDriver"; // Hive driver
    static Logger logger = Logger.getAnonymousLogger(); // logging event
    static Connection connect;

    // various analysis tasks
    public void analysis1() throws SQLException {

        try {
            Class.forName(driver);
            connect = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "", ""); // connecting to
                                                                                                   // default database
            Statement stmt = connect.createStatement();

            // Query 1
            String sql1 = "SELECT COUNT(uri) AS cnt, user_id FROM logs GROUP BY user_id CLUSTER BY cnt";
            ResultSet res1 = stmt.executeQuery(sql1);

            System.out.println("Executing query: " + sql1 + "\n");

            while (res1.next()) {
                System.out.println(res1.getString(1) + "\t" + res1.getString("cnt"));

            }

            // Query 2
            String sql2 = "select uri, count(uri) as uri_total_hits, round(size / 1024) as data_size_in_KB from logs group by uri, size order by uri_total_hits desc";
            ResultSet res2 = stmt.executeQuery(sql2);

            System.out.println("Executing query: " + sql1 + "\n");

            while (res2.next()) {
                System.out.println(res2.getString(1) + "\t" + res2.getString("uri_total_hits") + "\t"
                        + res2.getString("data_size_in_KB"));

            }

            System.out.println("\n**Done**");

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Can't recovered exception. ", e);
            e.printStackTrace();
            System.exit(1);
        } finally {
            connect.close(); // safely close connection
            System.out.println("Connection closed.");
        }

    }

    public static void main(String[] args) throws SQLException {

        Hive_Java obj = new Hive_Java();
        obj.analysis1();
    }// end main
}// end class
