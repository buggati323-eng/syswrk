package uk.ac.ntu.cloudfs.lb.term;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public final class NanoDao {

    public void open(Connection sqlite, String user, String cwd, String path, String initial) throws Exception {
        try (PreparedStatement ps = sqlite.prepareStatement(
                "REPLACE INTO nano_session(username,path,cwd,buffer) VALUES(?,?,?,?)")) {
            ps.setString(1, user);
            ps.setString(2, path);
            ps.setString(3, cwd);
            ps.setString(4, initial == null ? "" : initial);
            ps.executeUpdate();
        }
    }

    public Row get(Connection sqlite, String user) throws Exception {
        try (PreparedStatement ps = sqlite.prepareStatement(
                "SELECT path,cwd,buffer FROM nano_session WHERE username=?")) {
            ps.setString(1, user);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                return new Row(rs.getString(1), rs.getString(2), rs.getString(3));
            }
        }
    }

    public void setBuffer(Connection sqlite, String user, String buf) throws Exception {
        try (PreparedStatement ps = sqlite.prepareStatement(
                "UPDATE nano_session SET buffer=? WHERE username=?")) {
            ps.setString(1, buf == null ? "" : buf);
            ps.setString(2, user);
            ps.executeUpdate();
        }
    }

    public void appendLine(Connection sqlite, String user, String line) throws Exception {
        Row r = get(sqlite, user);
        if (r == null) return;
        String b = r.buffer();
        if (b == null || b.isEmpty()) b = "";
        if (!b.isEmpty() && !b.endsWith("\n")) b += "\n";
        b += (line == null ? "" : line);
        setBuffer(sqlite, user, b);
    }

    public void close(Connection sqlite, String user) throws Exception {
        try (PreparedStatement ps = sqlite.prepareStatement(
                "DELETE FROM nano_session WHERE username=?")) {
            ps.setString(1, user);
            ps.executeUpdate();
        }
    }

    public record Row(String path, String cwd, String buffer) {}
}