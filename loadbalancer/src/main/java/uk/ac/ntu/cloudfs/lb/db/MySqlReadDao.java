package uk.ac.ntu.cloudfs.lb.db;

import uk.ac.ntu.cloudfs.lb.core.ChunkPlacement;
import uk.ac.ntu.cloudfs.lb.core.FileManifestStore;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

public final class MySqlReadDao {

    public List<FileManifestStore.Manifest> loadAllManifests(Connection mysql) throws Exception {
        Map<String, FileManifestStore.Manifest> out = new LinkedHashMap<>();

        // files
        try (PreparedStatement ps = mysql.prepareStatement("SELECT file_id,total_bytes,chunk_size FROM files");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String fileId = rs.getString(1);
                long total = rs.getLong(2);
                int chunkSize = rs.getInt(3);
                out.put(fileId, new FileManifestStore.Manifest(fileId, total, chunkSize, new ArrayList<>()));
            }
        }

        // file_chunks (ordered)
        try (PreparedStatement ps = mysql.prepareStatement(
                "SELECT file_id,chunk_id FROM file_chunks ORDER BY file_id,chunk_index");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String fileId = rs.getString(1);
                String chunkId = rs.getString(2);
                FileManifestStore.Manifest m = out.get(fileId);
                if (m != null) {
                    ((List<String>) m.chunkIds()).add(chunkId);
                }
            }
        }

        return new ArrayList<>(out.values());
    }

    public List<PlacementRow> loadAllPlacements(Connection mysql) throws Exception {
        List<PlacementRow> rows = new ArrayList<>();
        try (PreparedStatement ps = mysql.prepareStatement(
                "SELECT file_id,chunk_id,replica_urls,crc32,bytes FROM chunk_placement");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String fileId = rs.getString(1);
                String chunkId = rs.getString(2);
                String replicaUrls = rs.getString(3);
                long crc32 = rs.getLong(4);
                int bytes = rs.getInt(5);

                List<String> urls = replicaUrls == null || replicaUrls.isBlank()
                        ? List.of()
                        : Arrays.asList(replicaUrls.split(","));

                rows.add(new PlacementRow(fileId, chunkId, urls, crc32, bytes));
            }
        }
        return rows;
    }

    public record PlacementRow(String fileId, String chunkId, List<String> urls, long crc32, int bytes) {}
}