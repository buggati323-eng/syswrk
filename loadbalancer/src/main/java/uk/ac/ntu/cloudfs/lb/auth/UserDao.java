package uk.ac.ntu.cloudfs.lb.auth;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public final class UserDao {

    public record UserRow(String username, String role, String createdAt) {}

    // Backwards-compatible create (defaults to STANDARD)
    public boolean createUser(Connection mysql, String username, String passwordHash) throws Exception {
        return createUser(mysql, username, passwordHash, "STANDARD");
    }

    public boolean createUser(Connection mysql, String username, String passwordHash, String role) throws Exception {
        try (PreparedStatement ps = mysql.prepareStatement(
                "INSERT INTO users(username,password_hash,role) VALUES(?,?,?)")) {
            ps.setString(1, username);
            ps.setString(2, passwordHash);
            ps.setString(3, normRole(role));
            ps.executeUpdate();
            return true;
        } catch (Exception e) {
            return false; // duplicate, etc.
        }
    }

    public String getPasswordHash(Connection mysql, String username) throws Exception {
        try (PreparedStatement ps = mysql.prepareStatement(
                "SELECT password_hash FROM users WHERE username=?")) {
            ps.setString(1, username);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }

    public String getRole(Connection mysql, String username) throws Exception {
        try (PreparedStatement ps = mysql.prepareStatement(
                "SELECT role FROM users WHERE username=?")) {
            ps.setString(1, username);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }

    public AuthRow getAuth(Connection mysql, String username) throws Exception {
        try (PreparedStatement ps = mysql.prepareStatement(
                "SELECT password_hash, role FROM users WHERE username=?")) {
            ps.setString(1, username);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                return new AuthRow(rs.getString(1), rs.getString(2));
            }
        }
    }

    public record AuthRow(String passwordHash, String role) {}

    public long countUsers(Connection mysql) throws Exception {
        try (PreparedStatement ps = mysql.prepareStatement("SELECT COUNT(*) FROM users");
             ResultSet rs = ps.executeQuery()) {
            rs.next();
            return rs.getLong(1);
        }
    }

    public boolean setRole(Connection mysql, String username, String role) throws Exception {
        try (PreparedStatement ps = mysql.prepareStatement(
                "UPDATE users SET role=? WHERE username=?")) {
            ps.setString(1, normRole(role));
            ps.setString(2, username);
            return ps.executeUpdate() == 1;
        }
    }

    public boolean deleteUser(Connection mysql, String username) throws Exception {
        try (PreparedStatement ps = mysql.prepareStatement(
                "DELETE FROM users WHERE username=?")) {
            ps.setString(1, username);
            return ps.executeUpdate() == 1;
        }
    }

    public List<UserRow> listUsers(Connection mysql) throws Exception {
        try (PreparedStatement ps = mysql.prepareStatement(
                "SELECT username, role, created_at FROM users ORDER BY created_at ASC");
             ResultSet rs = ps.executeQuery()) {
            List<UserRow> out = new ArrayList<>();
            while (rs.next()) {
                out.add(new UserRow(rs.getString(1), rs.getString(2), rs.getString(3)));
            }
            return out;
        }
    }

    private static String normRole(String role) {
        if (role == null) return "STANDARD";
        String r = role.trim().toUpperCase();
        if (!r.equals("ADMIN") && !r.equals("STANDARD")) return "STANDARD";
        return r;
    }
}