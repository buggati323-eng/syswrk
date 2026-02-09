package uk.ac.ntu.cloudfs.lb.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public final class Db {
    private Db() {}

    static {
        // Ensures drivers are registered even in shaded jar environments
        try { Class.forName("org.sqlite.JDBC"); } catch (ClassNotFoundException ignored) {}
        try { Class.forName("com.mysql.cj.jdbc.Driver"); } catch (ClassNotFoundException ignored) {}
    }

    public static Connection mysql() throws SQLException {
        return DriverManager.getConnection(DbConfig.mysqlUrl(), DbConfig.mysqlUser(), DbConfig.mysqlPass());
    }

    public static Connection sqlite() throws SQLException {
        return DriverManager.getConnection("jdbc:sqlite:" + DbConfig.sqlitePath());
    }
}