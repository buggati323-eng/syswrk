package uk.ac.ntu.cloudfs.lb.db;

import uk.ac.ntu.cloudfs.lb.core.ChunkPlacement;
import uk.ac.ntu.cloudfs.lb.core.FileManifestStore;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.StringJoiner;

public final class FileDao {

    public void upsertFile(Connection c, FileManifestStore.Manifest m) throws Exception {
        try (PreparedStatement ps = c.prepareStatement(
                "REPLACE INTO files(file_id,total_bytes,chunk_size) VALUES(?,?,?)")) {
            ps.setString(1, m.fileId());
            ps.setLong(2, m.totalBytes());
            ps.setInt(3, m.chunkSize());
            ps.executeUpdate();
        }

        try (PreparedStatement del = c.prepareStatement("DELETE FROM file_chunks WHERE file_id=?")) {
            del.setString(1, m.fileId());
            del.executeUpdate();
        }

        try (PreparedStatement ins = c.prepareStatement(
                "INSERT INTO file_chunks(file_id,chunk_index,chunk_id) VALUES(?,?,?)")) {
            int idx = 0;
            for (String chunkId : m.chunkIds()) {
                ins.setString(1, m.fileId());
                ins.setInt(2, idx++);
                ins.setString(3, chunkId);
                ins.addBatch();
            }
            ins.executeBatch();
        }
    }

        public void deleteFile(Connection mysql, String fileId) throws Exception {
    try (var ps = mysql.prepareStatement("DELETE FROM files WHERE file_id=?")) {
        ps.setString(1, fileId);
        ps.executeUpdate();
    }
    try (var ps = mysql.prepareStatement("DELETE FROM placements WHERE file_id=?")) {
        ps.setString(1, fileId);
        ps.executeUpdate();
    }
}

    public void upsertPlacement(Connection c, String fileId, String chunkId, ChunkPlacement.Entry e) throws Exception {
        StringJoiner j = new StringJoiner(",");
        for (String url : e.nodeBaseUrls()) j.add(url);

        try (PreparedStatement ps = c.prepareStatement(
                "REPLACE INTO chunk_placement(file_id,chunk_id,replica_urls,crc32,bytes) VALUES(?,?,?,?,?)")) {
            ps.setString(1, fileId);
            ps.setString(2, chunkId);
            ps.setString(3, j.toString());
            ps.setLong(4, e.crc32());
            ps.setInt(5, e.bytes());
            ps.executeUpdate();
        }
    }
}