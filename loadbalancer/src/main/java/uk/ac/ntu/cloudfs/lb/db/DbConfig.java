package uk.ac.ntu.cloudfs.lb.db;

public final class DbConfig {
    private DbConfig() {}

    public static String mysqlUrl() {
        return System.getenv().getOrDefault("MYSQL_URL", "jdbc:mysql://localhost:3306/cloudfs");
    }
    public static String mysqlUser() {
        return System.getenv().getOrDefault("MYSQL_USER", "cloudfs");
    }
    public static String mysqlPass() {
        return System.getenv().getOrDefault("MYSQL_PASS", "cloudfs");
    }

    public static String sqlitePath() {
        return System.getenv().getOrDefault("SQLITE_PATH", "./cloudfs_cache.db");
    }
}