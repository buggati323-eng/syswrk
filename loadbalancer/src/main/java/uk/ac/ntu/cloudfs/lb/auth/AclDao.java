package uk.ac.ntu.cloudfs.lb.auth;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public final class AclDao {

    public void ensureOwnerFullAccess(Connection mysql, String fileId, String ownerUsername) throws Exception {
        try (PreparedStatement ps = mysql.prepareStatement(
                "REPLACE INTO acls(file_id,username,can_read,can_write) VALUES(?,?,true,true)")) {
            ps.setString(1, fileId);
            ps.setString(2, ownerUsername);
            ps.executeUpdate();
        }
    }


    public void deleteAllForFile(Connection mysql, String fileId) throws Exception {
    try (var ps = mysql.prepareStatement("DELETE FROM acls WHERE file_id=?")) {
        ps.setString(1, fileId);
        ps.executeUpdate();
    }
}


    public boolean canRead(Connection mysql, String fileId, String username) throws Exception {
        return perm(mysql, fileId, username, "can_read");
    }

    public boolean canWrite(Connection mysql, String fileId, String username) throws Exception {
        return perm(mysql, fileId, username, "can_write");
    }

    private boolean perm(Connection mysql, String fileId, String username, String col) throws Exception {
        try (PreparedStatement ps = mysql.prepareStatement(
                "SELECT " + col + " FROM acls WHERE file_id=? AND username=?")) {
            ps.setString(1, fileId);
            ps.setString(2, username);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getBoolean(1);
            }
        }
    }
    
    public void grant(Connection mysql, String fileId, String targetUsername, boolean canRead, boolean canWrite) throws Exception {
    try (PreparedStatement ps = mysql.prepareStatement(
            "REPLACE INTO acls(file_id,username,can_read,can_write) VALUES(?,?,?,?)")) {
        ps.setString(1, fileId);
        ps.setString(2, targetUsername);
        ps.setBoolean(3, canRead);
        ps.setBoolean(4, canWrite);
        ps.executeUpdate();
    }
}

public void revoke(Connection mysql, String fileId, String targetUsername) throws Exception {
    try (PreparedStatement ps = mysql.prepareStatement(
            "DELETE FROM acls WHERE file_id=? AND username=?")) {
        ps.setString(1, fileId);
        ps.setString(2, targetUsername);
        ps.executeUpdate();
    }
}
}
