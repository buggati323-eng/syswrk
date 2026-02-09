package uk.ac.ntu.cloudfs.lb.db;

import uk.ac.ntu.cloudfs.lb.core.FileManifestStore;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public final class SqliteCacheDao {

    public void upsertCachedFile(Connection sqlite, FileManifestStore.Manifest m) throws Exception {
        try (PreparedStatement ps = sqlite.prepareStatement(
                "REPLACE INTO cached_files(file_id,total_bytes,chunk_size,updated_at) VALUES(?,?,?,datetime('now'))")) {
            ps.setString(1, m.fileId());
            ps.setLong(2, m.totalBytes());
            ps.setInt(3, m.chunkSize());
            ps.executeUpdate();
        }
    }

    public void deleteCachedFile(Connection sqlite, String fileId) throws Exception {
    try (PreparedStatement ps = sqlite.prepareStatement("DELETE FROM cached_files WHERE file_id=?")) {
        ps.setString(1, fileId);
        ps.executeUpdate();
    }
}

    public List<String> listFileIds(Connection sqlite) throws Exception {
        List<String> out = new ArrayList<>();
        try (PreparedStatement ps = sqlite.prepareStatement("SELECT file_id FROM cached_files ORDER BY updated_at DESC");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) out.add(rs.getString(1));
        }
        return out;
    }
}