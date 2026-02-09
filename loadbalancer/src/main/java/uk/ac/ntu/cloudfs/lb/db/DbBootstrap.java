package uk.ac.ntu.cloudfs.lb.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;

public final class DbBootstrap {
    private static final Logger log = LoggerFactory.getLogger(DbBootstrap.class);

    private DbBootstrap() {}

    public static DbHandles init() {
        // Always init SQLite schema
        try (Connection sqlite = Db.sqlite()) {
            Schema.ensureSqlite(sqlite);
            log.info("SQLite ready at {}", DbConfig.sqlitePath());
        } catch (Exception e) {
            throw new RuntimeException("SQLite init failed: " + e.getMessage(), e);
        }

        // Try MySQL schema (non-fatal if down)
        boolean mysqlOk = false;
        try (Connection mysql = Db.mysql()) {
            Schema.ensureMySql(mysql);
            log.info("MySQL ready at {}", DbConfig.mysqlUrl());
            mysqlOk = true;
        } catch (Exception e) {
            log.warn("MySQL unavailable (continuing without it): {}", e.getMessage());
        }

        return new DbHandles(mysqlOk);
    }

    public record DbHandles(boolean mysqlAvailable) {}
}