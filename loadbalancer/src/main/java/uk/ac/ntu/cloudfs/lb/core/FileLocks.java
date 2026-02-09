package uk.ac.ntu.cloudfs.lb.core;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class FileLocks {
    private final ConcurrentHashMap<String, ReentrantReadWriteLock> locks = new ConcurrentHashMap<>();
    private ReentrantReadWriteLock lock(String fileId) {
        return locks.computeIfAbsent(fileId, k -> new ReentrantReadWriteLock());
    }

    public <T> T withRead(String fileId, ThrowingSupplier<T> s) throws Exception {
        var l = lock(fileId).readLock();
        l.lock();
        try { return s.get(); }
        finally { l.unlock(); }
    }

    public <T> T withWrite(String fileId, ThrowingSupplier<T> s) throws Exception {
        var l = lock(fileId).writeLock();
        l.lock();
        try { return s.get(); }
        finally { l.unlock(); }
    }

    @FunctionalInterface public interface ThrowingSupplier<T> { T get() throws Exception; }
}