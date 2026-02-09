package uk.ac.ntu.cloudfs.lb.term;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public final class VfsIndex {
    // key: username + ":" + fullPath
    private final ConcurrentHashMap<String, Entry> map = new ConcurrentHashMap<>();

    public record Entry(boolean isDir, String fileId) {
        public static Entry dir() { return new Entry(true, null); }
        public static Entry file(String fileId) { return new Entry(false, fileId); }
    }

    private static String k(String user, String path) { return user + ":" + path; }

    // ---------------- basic ops ----------------

    public void mkdir(String user, String path) {
        map.put(k(user, path), Entry.dir());
    }

    /** Creates all parent dirs (and the dir itself) like `mkdir -p`. */
    public void mkdirs(String user, String path) {
        if (path == null || path.isBlank() || "/".equals(path)) return;
        String[] parts = path.split("/");
        String cur = "";
        for (String p : parts) {
            if (p.isBlank()) continue;
            cur = cur + "/" + p;
            map.putIfAbsent(k(user, cur), Entry.dir());
        }
    }

    public void touch(String user, String path, String fileId) {
        if (path == null || path.isBlank() || "/".equals(path)) {
            throw new IllegalArgumentException("touch: invalid path");
        }

        synchronized (map) {
            // ensure parent exists and is dir
            String parent = path.contains("/") ? path.substring(0, path.lastIndexOf('/')) : "/";
            if (parent.isBlank()) parent = "/";
            if (!"/".equals(parent)) {
                Entry pe = map.get(k(user, parent));
                if (pe == null || !pe.isDir()) throw new IllegalArgumentException("touch: no such directory: " + parent);
            }

            // if already exists as dir -> error, if file -> no-op
            Entry e = map.get(k(user, path));
            if (e != null) {
                if (e.isDir()) throw new IllegalArgumentException("touch: is a directory: " + path);
                return;
            }

            map.put(k(user, path), Entry.file(fileId));
        }
    }

    public void putFile(String user, String path, String fileId) {
        map.put(k(user, path), Entry.file(fileId));
    }

    public Entry get(String user, String path) {
        return map.get(k(user, path));
    }

    public boolean exists(String user, String path) {
        return map.containsKey(k(user, path));
    }

    public List<String> listChildren(String user, String dirPath) {
        String prefix = user + ":" + (dirPath.endsWith("/") ? dirPath : dirPath + "/");
        List<String> out = new ArrayList<>();
        for (String key : map.keySet()) {
            if (!key.startsWith(prefix)) continue;
            String rest = key.substring(prefix.length());
            if (rest.isEmpty()) continue;
            // only direct children
            int slash = rest.indexOf('/');
            String name = (slash == -1) ? rest : rest.substring(0, slash);
            if (!out.contains(name)) out.add(name);
        }
        Collections.sort(out);
        return out;
    }

    // ---------------- mv / cp ----------------

    /**
     * Move src -> dst. Works for files and directories (moves subtree).
     * Destination must not already exist.
     */
    public void mv(String user, String src, String dst) {
        if ("/".equals(src)) throw new IllegalArgumentException("mv: cannot move /");
        if (src.equals(dst)) return;

        synchronized (map) {
            Entry srcEntry = map.get(k(user, src));
            if (srcEntry == null) throw new IllegalArgumentException("mv: no such file or directory: " + src);

            if (map.containsKey(k(user, dst))) {
                throw new IllegalArgumentException("mv: destination exists: " + dst);
            }

            // disallow moving a directory into itself, e.g. mv /a /a/b
            if (srcEntry.isDir() && (dst.equals(src) || dst.startsWith(src + "/"))) {
                throw new IllegalArgumentException("mv: cannot move a directory into itself");
            }

            // ensure destination parent dirs exist
            String parent = parentOf(dst);
            if (parent != null && !"/".equals(parent) && !isDir(user, parent)) {
                throw new IllegalArgumentException("mv: destination parent does not exist: " + parent);
            }

            if (!srcEntry.isDir()) {
                // file move
                map.remove(k(user, src));
                map.put(k(user, dst), srcEntry);
                return;
            }

            // dir move: move the dir entry + all subtree entries
            String srcKeyPrefix = k(user, src);
            String srcPrefix = user + ":" + (src.endsWith("/") ? src : src + "/");

            // collect keys to move
            List<String> keys = new ArrayList<>();
            for (String key : map.keySet()) {
                if (key.equals(srcKeyPrefix) || key.startsWith(srcPrefix)) keys.add(key);
            }

            // move entries
            for (String key : keys) {
                Entry e = map.remove(key);
                if (e == null) continue;

                String oldPath = key.substring((user + ":").length());
                String rel = oldPath.equals(src) ? "" : oldPath.substring(src.length()); // includes leading "/..."
                String newPath = dst + rel;

                map.put(k(user, newPath), e);
            }
        }
    }

    /*
     * NOTE: For files, this copies the VFS entry and keeps the same fileId.
     * It does NOT duplicate bytes in storage.
     */
    public void cp(String user, String src, String dst) {
        if (src.equals(dst)) return;

        synchronized (map) {
            Entry srcEntry = map.get(k(user, src));
            if (srcEntry == null) throw new IllegalArgumentException("cp: no such file or directory: " + src);

            if (map.containsKey(k(user, dst))) {
                throw new IllegalArgumentException("cp: destination exists: " + dst);
            }

            // ensure destination parent exists
            String parent = parentOf(dst);
            if (parent != null && !"/".equals(parent) && !isDir(user, parent)) {
                throw new IllegalArgumentException("cp: destination parent does not exist: " + parent);
            }

            if (!srcEntry.isDir()) {
                // file copy (same fileId reference)
                map.put(k(user, dst), Entry.file(srcEntry.fileId()));
                return;
            }

            // dir copy: copy the dir entry + all subtree entries
            String srcKeyPrefix = k(user, src);
            String srcPrefix = user + ":" + (src.endsWith("/") ? src : src + "/");

            // collect keys to copy
            List<String> keys = new ArrayList<>();
            for (String key : map.keySet()) {
                if (key.equals(srcKeyPrefix) || key.startsWith(srcPrefix)) keys.add(key);
            }

            // copy entries
            for (String key : keys) {
                Entry e = map.get(key);
                if (e == null) continue;

                String oldPath = key.substring((user + ":").length());
                String rel = oldPath.equals(src) ? "" : oldPath.substring(src.length()); // includes leading "/..."
                String newPath = dst + rel;

                map.put(k(user, newPath), e);
            }
        }
    }

    public void linkFile(String user, String path, String fileId) {
        // same as touch, but uses existing fileId (does NOT create a new one)
        if (path == null || path.isBlank() || "/".equals(path)) {
            throw new IllegalArgumentException("link: invalid path");
        }
        synchronized (map) {
            // allow overwrite? keep it safe: if exists, do nothing
            Entry existing = map.get(k(user, path));
            if (existing != null) {
                if (existing.isDir()) throw new IllegalArgumentException("link: is a directory: " + path);
                return;
            }
            map.put(k(user, path), Entry.file(fileId));
        }
    }

    public void rm(String user, String path) {
        if (path == null || path.isBlank() || "/".equals(path)) {
            throw new IllegalArgumentException("rm: refusing to remove /");
        }

        synchronized (map) {
            Entry e = map.get(k(user, path));
            if (e == null) throw new IllegalArgumentException("rm: no such file or directory: " + path);

            if (!e.isDir()) {
                map.remove(k(user, path));
                return;
            }

            // directory: remove subtree
            String prefix = user + ":" + (path.endsWith("/") ? path : path + "/");
            List<String> keys = new ArrayList<>();
            for (String key : map.keySet()) {
                if (key.equals(k(user, path)) || key.startsWith(prefix)) keys.add(key);
            }
            for (String key : keys) map.remove(key);
        }
    }

    private boolean isDir(String user, String path) {
        if ("/".equals(path)) return true;
        Entry e = map.get(k(user, path));
        return e != null && e.isDir();
    }

    private static String parentOf(String path) {
        if (path == null || path.isBlank() || "/".equals(path)) return null;
        int idx = path.lastIndexOf('/');
        if (idx <= 0) return "/";
        return path.substring(0, idx);
    }
}