import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DataBaseConnection {

public static Connection conections(String url1, String user, String password) {
    Connection con = null;

    try {
        con = DriverManager.getConnection(url1, user, password);
        if (con != null) {
            System.out.println("Connected to the database");
        }
    } catch (SQLException throwables) {
        throwables.printStackTrace();
    }
    return con;
}
}











