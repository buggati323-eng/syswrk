package uk.ac.ntu.cloudfs.lb.audit;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public final class AuditDao {

    public void log(Connection mysql, String username, String action, String detail, boolean ok, String remote) {
        try (PreparedStatement ps = mysql.prepareStatement(
                "INSERT INTO audit_log(username,action,detail,ok,remote) VALUES(?,?,?,?,?)")) {
            ps.setString(1, username);
            ps.setString(2, action);
            ps.setString(3, detail);
            ps.setInt(4, ok ? 1 : 0);
            ps.setString(5, remote);
            ps.executeUpdate();
        } catch (Exception ignored) {
            // audit logging should never break requests
        }
    }

    public List<String> latest(Connection mysql, int limit) throws Exception {
        try (PreparedStatement ps = mysql.prepareStatement(
                "SELECT ts, username, action, ok, remote, detail FROM audit_log ORDER BY id DESC LIMIT ?")) {
            ps.setInt(1, Math.max(1, Math.min(limit, 500)));
            try (ResultSet rs = ps.executeQuery()) {
                List<String> out = new ArrayList<>();
                while (rs.next()) {
                    out.add(rs.getString(1) + " | " +
                            n(rs.getString(2)) + " | " +
                            rs.getString(3) + " | " +
                            (rs.getInt(4) == 1 ? "OK" : "FAIL") + " | " +
                            n(rs.getString(5)) + " | " +
                            n(rs.getString(6)));
                }
                return out;
            }
        }
    }

    private static String n(String s) { return s == null ? "-" : s; }
}
