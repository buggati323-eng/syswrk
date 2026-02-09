package uk.ac.ntu.cloudfs.node.store;

import java.io.IOException;
import java.nio.file.*;
import java.util.zip.CRC32;

public final class ChunkStore {
    private final Path baseDir;

    public ChunkStore(Path baseDir) throws IOException {
        this.baseDir = baseDir;
        Files.createDirectories(baseDir);
    }

    public PutResult put(String fileId, String chunkId, byte[] data) throws IOException {
        Path dir = baseDir.resolve(safe(fileId));
        Files.createDirectories(dir);

        Path p = dir.resolve(safe(chunkId) + ".bin");
        Files.write(p, data, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        long crc = crc32(data);
        return new PutResult(data.length, crc);
    }

    public byte[] get(String fileId, String chunkId) throws IOException {
        Path p = baseDir.resolve(safe(fileId)).resolve(safe(chunkId) + ".bin");
        return Files.readAllBytes(p);
    }

    public void deleteFile(String fileId) throws IOException {
        Path dir = baseDir.resolve(safe(fileId));
        if (!Files.exists(dir)) return;
        try (var s = Files.list(dir)) {
            s.forEach(path -> {
                try { Files.deleteIfExists(path); } catch (IOException ignored) {}
            });
        }
        Files.deleteIfExists(dir);
    }

    private static String safe(String s) {
        // simple sanitiser: keep alphanum, dash, underscore
        return s.replaceAll("[^a-zA-Z0-9_-]", "_");
    }

    private static long crc32(byte[] data) {
        CRC32 crc = new CRC32();
        crc.update(data);
        return crc.getValue();
    }

    public record PutResult(int bytes, long crc32) {}
}