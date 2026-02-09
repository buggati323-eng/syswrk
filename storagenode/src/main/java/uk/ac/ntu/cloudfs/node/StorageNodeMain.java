package uk.ac.ntu.cloudfs.node;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ntu.cloudfs.common.Version;
import uk.ac.ntu.cloudfs.node.store.ChunkStore;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

public final class StorageNodeMain {

    private static final Logger log = LoggerFactory.getLogger(StorageNodeMain.class);

    public static void main(String[] args) throws IOException {
        int port = readIntEnv("NODE_PORT", 9001);

        String dataDir = System.getenv().getOrDefault("NODE_DATA_DIR", "./data");
        ChunkStore store = new ChunkStore(Paths.get(dataDir));

        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);

        // ---- basic endpoints ----
        server.createContext("/health", ex -> reply(ex, 200, "OK"));
        server.createContext("/version", ex -> reply(ex, 200, Version.NAME + " " + Version.VERSION));

        // ---- delay demo endpoint ----
        server.createContext("/ping", ex -> {
            long delayMs = readLongEnv("NODE_DELAY_MS", 0);
            if (delayMs > 0) {
                try { Thread.sleep(delayMs); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }
            reply(ex, 200, "PONG from " + System.getenv().getOrDefault("NODE_ID", "node"));
        });

        // ---- chunk storage endpoint: PUT + GET on same path ----
        server.createContext("/chunk", ex -> {
            String method = ex.getRequestMethod().toUpperCase();
            String q = ex.getRequestURI().getQuery();
            String fileId = queryParam(q, "fileId");
            String chunkId = queryParam(q, "chunkId");

            if (fileId == null || chunkId == null) {
                reply(ex, 400, "MISSING fileId/chunkId");
                return;
            }

            if ("PUT".equals(method)) {
                long delayMs = readLongEnv("NODE_DELAY_MS", 0);
                if (delayMs > 0) {
                    try { Thread.sleep(delayMs); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                }

                byte[] data = ex.getRequestBody().readAllBytes();
                try {
                    ChunkStore.PutResult res = store.put(fileId, chunkId, data);
                    reply(ex, 200, "STORED bytes=" + res.bytes() + " crc32=" + res.crc32());
                } catch (IOException io) {
                    reply(ex, 500, "STORE_ERROR " + io.getMessage());
                }
                return;
            }

            if ("GET".equals(method)) {
                try {
                    byte[] data = store.get(fileId, chunkId);
                    ex.getResponseHeaders().set("Content-Type", "application/octet-stream");
                    ex.sendResponseHeaders(200, data.length);
                    try (OutputStream os = ex.getResponseBody()) {
                        os.write(data);
                    }
                } catch (IOException io) {
                    reply(ex, 404, "NOT_FOUND");
                }
                return;
            }

            reply(ex, 405, "METHOD_NOT_ALLOWED");
        });

        // ---- delete all chunks for a file ----
        server.createContext("/file", ex -> {
            if (!"DELETE".equalsIgnoreCase(ex.getRequestMethod())) {
                reply(ex, 405, "METHOD_NOT_ALLOWED");
                return;
            }
            String q = ex.getRequestURI().getQuery();
            String fileId = queryParam(q, "fileId");
            if (fileId == null) {
                reply(ex, 400, "MISSING fileId");
                return;
            }
            try {
                store.deleteFile(fileId);
                reply(ex, 200, "DELETED " + fileId);
            } catch (IOException io) {
                reply(ex, 500, "DELETE_ERROR " + io.getMessage());
            }
        });

        server.setExecutor(null);
        server.start();

        log.info("StorageNode started on port {} (endpoints: /health, /version, /ping, /chunk, /file)", port);
        log.info("Data dir: {}", Paths.get(dataDir).toAbsolutePath());
    }

    private static int readIntEnv(String key, int fallback) {
        String v = System.getenv(key);
        if (v == null || v.isBlank()) return fallback;
        try { return Integer.parseInt(v.trim()); }
        catch (NumberFormatException e) { return fallback; }
    }

    private static long readLongEnv(String key, long fallback) {
        String v = System.getenv(key);
        if (v == null || v.isBlank()) return fallback;
        try { return Long.parseLong(v.trim()); }
        catch (NumberFormatException e) { return fallback; }
    }

    private static String queryParam(String query, String key) {
        if (query == null) return null;
        for (String part : query.split("&")) {
            String[] kv = part.split("=", 2);
            if (kv.length == 2 && kv[0].equals(key)) return kv[1];
        }
        return null;
    }

    private static void reply(HttpExchange ex, int code, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
        ex.sendResponseHeaders(code, bytes.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
        }
    }
}