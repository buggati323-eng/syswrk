package uk.ac.ntu.cloudfs.lb.db;

import java.sql.Connection;
import java.sql.Statement;

public final class Schema {
    private Schema() {}

    public static void ensureMySql(Connection c) throws Exception {
        try (Statement s = c.createStatement()) {

            s.executeUpdate("""
                CREATE TABLE IF NOT EXISTS files (
                  file_id VARCHAR(128) PRIMARY KEY,
                  total_bytes BIGINT NOT NULL,
                  chunk_size INT NOT NULL,
                  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                    ON UPDATE CURRENT_TIMESTAMP
                )
            """);

            s.executeUpdate("""
                CREATE TABLE IF NOT EXISTS file_chunks (
                  file_id VARCHAR(128) NOT NULL,
                  chunk_index INT NOT NULL,
                  chunk_id VARCHAR(128) NOT NULL,
                  PRIMARY KEY (file_id, chunk_index),
                  FOREIGN KEY (file_id) REFERENCES files(file_id)
                    ON DELETE CASCADE
                )
            """);

            s.executeUpdate("""
                CREATE TABLE IF NOT EXISTS chunk_placement (
                  file_id VARCHAR(128) NOT NULL,
                  chunk_id VARCHAR(128) NOT NULL,
                  replica_urls TEXT NOT NULL,
                  crc32 BIGINT NOT NULL,
                  bytes INT NOT NULL,
                  PRIMARY KEY (file_id, chunk_id)
                )
            """);

            s.executeUpdate("""
            CREATE TABLE IF NOT EXISTS audit_log (
              id BIGINT PRIMARY KEY AUTO_INCREMENT,
              ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
              username VARCHAR(64),
              action VARCHAR(64) NOT NULL,
              detail VARCHAR(512),
              ok TINYINT NOT NULL,
              remote VARCHAR(64)
            )
            """);

            s.executeUpdate("""
                CREATE TABLE IF NOT EXISTS users (
                  user_id BIGINT AUTO_INCREMENT PRIMARY KEY,
                  username VARCHAR(64) NOT NULL UNIQUE,
                  password_hash VARCHAR(255) NOT NULL,
                  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """);

            try {
                s.executeUpdate("""
                    ALTER TABLE users
                    ADD COLUMN role VARCHAR(16)
                    NOT NULL DEFAULT 'STANDARD'
                """);
            } catch (Exception ignored) {
                // column should exitst
            }

            s.executeUpdate("""
                CREATE TABLE IF NOT EXISTS acls (
                  file_id VARCHAR(128) NOT NULL,
                  username VARCHAR(64) NOT NULL,
                  can_read BOOLEAN NOT NULL,
                  can_write BOOLEAN NOT NULL,
                  PRIMARY KEY (file_id, username),
                  FOREIGN KEY (file_id) REFERENCES files(file_id)
                    ON DELETE CASCADE,
                  FOREIGN KEY (username) REFERENCES users(username)
                    ON DELETE CASCADE
                )
            """);
        }
    }

    public static void ensureSqlite(Connection c) throws Exception {
        try (Statement s = c.createStatement()) {

            s.executeUpdate("""
                CREATE TABLE IF NOT EXISTS cached_files (
                  file_id TEXT PRIMARY KEY,
                  total_bytes INTEGER NOT NULL,
                  chunk_size INTEGER NOT NULL,
                  updated_at TEXT NOT NULL
                )
            """);

            s.executeUpdate("""
                CREATE TABLE IF NOT EXISTS cwd (
                  username TEXT PRIMARY KEY,
                  path TEXT NOT NULL
                )
            """);

            s.executeUpdate("""
                CREATE TABLE IF NOT EXISTS sync_state (
                  id INTEGER PRIMARY KEY CHECK (id=1),
                  last_sync TEXT
                )
            """);

            s.executeUpdate("""
                INSERT OR IGNORE INTO sync_state(id, last_sync)
                VALUES (1, NULL)
            """);

            s.executeUpdate("""
                CREATE TABLE IF NOT EXISTS sessions (
                  token TEXT PRIMARY KEY,
                  username TEXT NOT NULL,
                  created_at TEXT NOT NULL
                )
            """);

            s.executeUpdate("""
            CREATE TABLE IF NOT EXISTS nano_session (
              username TEXT NOT NULL,
              path TEXT NOT NULL,
              cwd TEXT NOT NULL,
              buffer TEXT NOT NULL,
              PRIMARY KEY(username)
            )
            """);
        }
    }
}