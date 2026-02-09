package uk.ac.ntu.cloudfs.lb.term;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public final class CwdDao {
    public String getOrInit(Connection sqlite, String username) throws Exception {
        try (PreparedStatement ps = sqlite.prepareStatement("SELECT path FROM cwd WHERE username=?")) {
            ps.setString(1, username);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getString(1);
            }
        }
        try (PreparedStatement ins = sqlite.prepareStatement("INSERT INTO cwd(username,path) VALUES(?,?)")) {
            ins.setString(1, username);
            ins.setString(2, "/");
            ins.executeUpdate();
        }
        return "/";
    }

    public void set(Connection sqlite, String username, String path) throws Exception {
        try (PreparedStatement ps = sqlite.prepareStatement("REPLACE INTO cwd(username,path) VALUES(?,?)")) {
            ps.setString(1, username);
            ps.setString(2, path);
            ps.executeUpdate();
        }
    }
}