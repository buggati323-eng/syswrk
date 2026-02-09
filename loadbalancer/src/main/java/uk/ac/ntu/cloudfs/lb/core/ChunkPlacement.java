package uk.ac.ntu.cloudfs.lb.core;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public final class ChunkPlacement {

    private final ConcurrentHashMap<String, Entry> map = new ConcurrentHashMap<>();

    private static String key(String fileId, String chunkId) {
        return fileId + "::" + chunkId;
    }

    public void remove(String fileId, String chunkId) {
    map.remove(key(fileId, chunkId));
    }

    public void put(String fileId, String chunkId, Entry entry) {
        map.put(key(fileId, chunkId), entry);
    }

    public Entry get(String fileId, String chunkId) {
        return map.get(key(fileId, chunkId));
    }

    public record Entry(List<String> nodeBaseUrls, long crc32, int bytes) {

    }
}
