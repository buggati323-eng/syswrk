package uk.ac.ntu.cloudfs.lb.auth;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.UUID;

public final class SessionDao {

    public String newSession(Connection sqlite, String username) throws Exception {
        String token = UUID.randomUUID().toString();
        try (PreparedStatement ps = sqlite.prepareStatement(
                "INSERT INTO sessions(token,username,created_at) VALUES(?,?,datetime('now'))")) {
            ps.setString(1, token);
            ps.setString(2, username);
            ps.executeUpdate();
        }
        return token;
    }

    public String usernameFor(Connection sqlite, String token) throws Exception {
        try (PreparedStatement ps = sqlite.prepareStatement(
                "SELECT username FROM sessions WHERE token=?")) {
            ps.setString(1, token);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }
}
