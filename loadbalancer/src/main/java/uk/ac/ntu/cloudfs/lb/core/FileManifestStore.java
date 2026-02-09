package uk.ac.ntu.cloudfs.lb.core;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public final class FileManifestStore {
    private final ConcurrentHashMap<String, Manifest> files = new ConcurrentHashMap<>();

    public void put(String fileId, Manifest manifest) {
        files.put(fileId, manifest);
    }

    public Manifest get(String fileId) {
        return files.get(fileId);
    }

    public Manifest remove(String fileId) {
    return files.remove(fileId);
}

    public record Manifest(String fileId, long totalBytes, int chunkSize, List<String> chunkIds) {}
}