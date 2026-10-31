import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Minimal ConnectionFactory for the Blocking homework.
 * Change the settings below to match your local PostgreSQL install.
 * Do NOT commit this file (it contains your password) - add it to .gitignore.
 */
public class ConnectionFactory {
    public static final int POSTGRESQL = 0;

    private static final String HOST = "localhost";
    private static final int PORT = 5432;
    private static final String DB = "postgres";
    private static final String USER = "postgres";
    private static final String PASSWORD = "admin";

    // Blocking.java calls this outside a try/catch in run(),
    // so it must not throw a checked SQLException.
    public static Connection getDefaultParameterConnection(int dbType) {
        if (dbType != POSTGRESQL) {
            throw new IllegalArgumentException("Only PostgreSQL is supported");
        }
        String url = "jdbc:postgresql://" + HOST + ":" + PORT + "/" + DB;
        Properties props = new Properties();
        props.setProperty("user", USER);
        props.setProperty("password", PASSWORD);
        try {
            return DriverManager.getConnection(url, props);
        } catch (SQLException e) {
            throw new RuntimeException("Could not connect to " + url + ": " + e.getMessage(), e);
        }
    }
}
