package uk.ac.ntu.cloudfs.lb;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import uk.ac.ntu.cloudfs.common.Version;
import uk.ac.ntu.cloudfs.common.scheduler.NodeInfo;
import uk.ac.ntu.cloudfs.common.scheduler.Scheduler;
import uk.ac.ntu.cloudfs.lb.audit.AuditDao;
import uk.ac.ntu.cloudfs.lb.auth.AclDao;
import uk.ac.ntu.cloudfs.lb.auth.PasswordHasher;
import uk.ac.ntu.cloudfs.lb.auth.SessionDao;
import uk.ac.ntu.cloudfs.lb.auth.UserDao;
import uk.ac.ntu.cloudfs.lb.core.ChunkPlacement;
import uk.ac.ntu.cloudfs.lb.core.Chunker;
import uk.ac.ntu.cloudfs.lb.core.Crypto;
import uk.ac.ntu.cloudfs.lb.core.FileLocks;
import uk.ac.ntu.cloudfs.lb.core.FileManifestStore;
import uk.ac.ntu.cloudfs.lb.core.HealthChecker;
import uk.ac.ntu.cloudfs.lb.core.JobQueue;
import uk.ac.ntu.cloudfs.lb.core.NodeConfig;
import uk.ac.ntu.cloudfs.lb.core.NodeRegistry;
import uk.ac.ntu.cloudfs.lb.core.ProxyClient;
import uk.ac.ntu.cloudfs.lb.core.ReplicaPicker;
import uk.ac.ntu.cloudfs.lb.core.SchedulerFactory;
import uk.ac.ntu.cloudfs.lb.db.Db;
import uk.ac.ntu.cloudfs.lb.db.DbBootstrap;
import uk.ac.ntu.cloudfs.lb.db.FileDao;
import uk.ac.ntu.cloudfs.lb.db.MySqlReadDao;
import uk.ac.ntu.cloudfs.lb.db.SqliteCacheDao;
import uk.ac.ntu.cloudfs.lb.term.CwdDao;
import uk.ac.ntu.cloudfs.lb.term.NanoDao;
import uk.ac.ntu.cloudfs.lb.term.VfsIndex;

public final class LoadBalancerMain {
    private static final Logger log = LoggerFactory.getLogger(LoadBalancerMain.class);
    private static final String BUILD_TAG = "test";

    public static void main(String[] args) throws IOException {
        int port = readIntEnv("LB_PORT", 8080);
        int workers = readIntEnv("LB_WORKERS", 4);
        int qcap = readIntEnv("LB_QUEUE_CAP", 50);
        int replicas = readIntEnv("LB_REPLICAS", 2);

        String schedulerName = System.getenv().getOrDefault("LB_SCHEDULER", "round_robin");
        Scheduler scheduler = SchedulerFactory.create(schedulerName);

        NodeRegistry registry = new NodeRegistry();
        for (NodeInfo n : NodeConfig.fromEnv()) registry.addNode(n);

        Thread hc = new Thread(new HealthChecker(registry, 2000));
        hc.setDaemon(true);
        hc.start();

        JobQueue jobQueue = new JobQueue(workers, qcap);
        ProxyClient proxy = new ProxyClient();

        FileLocks fileLocks = new FileLocks();
        Crypto crypto = Crypto.fromEnv();

        ChunkPlacement placement = new ChunkPlacement();
        FileManifestStore manifests = new FileManifestStore();

        DbBootstrap.DbHandles dbh = DbBootstrap.init();
        FileDao fileDao = new FileDao();
        SqliteCacheDao cacheDao = new SqliteCacheDao();

        UserDao userDao = new UserDao();
        SessionDao sessionDao = new SessionDao();
        AclDao aclDao = new AclDao();

        AuditDao auditDao = new AuditDao();

        CwdDao cwdDao = new CwdDao();
        VfsIndex vfs = new VfsIndex();
        NanoDao nanoDao = new NanoDao();

        if (dbh.mysqlAvailable()) {
            try (var mysql = Db.mysql(); var sqlite = Db.sqlite()) {
                MySqlReadDao reader = new MySqlReadDao();

                for (var m : reader.loadAllManifests(mysql)) {
                    manifests.put(m.fileId(), m);
                    try { cacheDao.upsertCachedFile(sqlite, m); } catch (Exception ignored) {}
                }

                for (var row : reader.loadAllPlacements(mysql)) {
                    placement.put(row.fileId(), row.chunkId(),
                            new ChunkPlacement.Entry(row.urls(), row.crc32(), row.bytes()));
                }

                try {
                    int cached = cacheDao.listFileIds(sqlite).size();
                    log.info("Startup sync complete: cachedFiles={}", cached);
                } catch (Exception e) {
                    log.info("Startup sync complete");
                }
            } catch (Exception e) {
                log.warn("Startup sync failed: {}", e.getMessage());
            }
        }

        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);

        server.createContext("/health", ex -> reply(ex, 200, "OK"));
        server.createContext("/version", ex -> reply(ex, 200, Version.NAME + " " + Version.VERSION));

        server.createContext("/debug/endpoints", ex -> {
            if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) { reply(ex, 405, "METHOD_NOT_ALLOWED"); return; }
            reply(ex, 200,
                    "BUILD=" + BUILD_TAG + "\n" +
                            "/health\n/version\n/route\n/metrics\n" +
                            "/api/auth/register\n/api/auth/login\n" +
                            "/api/files\n/api/ping\n" +
                            "/api/acl/grant\n/api/acl/revoke\n" +
                            "/api/admin/users\n/api/admin/create\n/api/admin/role\n/api/admin/audit\n" +
                            "/api/chunk\n/api/file\n" +
                            "/api/term\n"
            );
        });

        server.createContext("/route", ex -> {
            var pick = scheduler.pick(registry.healthy());
            if (pick.isEmpty()) { reply(ex, 503, "NO_HEALTHY_NODES"); return; }
            NodeInfo n = pick.get();
            reply(ex, 200, scheduler.name() + " -> " + n.nodeId() + " " + n.baseUrl());
        });

        server.createContext("/metrics", ex -> {
            int healthy = registry.healthy().size();
            int total = registry.all().size();
            reply(ex, 200,
                    "scheduler=" + scheduler.name()
                            + " nodesHealthy=" + healthy + "/" + total
                            + " workers=" + workers
                            + " queued=" + jobQueue.queued() + "/" + jobQueue.capacity()
                            + " mysql=" + (dbh.mysqlAvailable() ? "up" : "down"));
        });

        // --- AUTH ---

        server.createContext("/api/auth/register", ex -> {
            String q = ex.getRequestURI().getQuery();
            String u = queryParam(q, "username");
            String p = queryParam(q, "password");
            if (u == null || p == null || u.isBlank() || p.isBlank()) {
                reply(ex, 400, "MISSING username/password");
                return;
            }

            String hash = PasswordHasher.hash(p.toCharArray());
            boolean ok;

            try (var mysql = Db.mysql()) {
                String role = "STANDARD";
                try {
                    long n = userDao.countUsers(mysql);
                    if (n == 0) role = "ADMIN"; // first user admin
                } catch (Exception ignored) {}
                ok = userDao.createUser(mysql, u, hash, role);
            } catch (Exception e) {
                ok = false;
            }

            if (dbh.mysqlAvailable()) {
                try (var mysql = Db.mysql()) {
                    auditDao.log(mysql, u, "REGISTER", "", ok, remote(ex));
                } catch (Exception ignored) {}
            }

            reply(ex, ok ? 200 : 409, ok ? "REGISTERED" : "USERNAME_TAKEN");
        });

        server.createContext("/api/auth/login", ex -> {
            if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) { reply(ex, 405, "METHOD_NOT_ALLOWED"); return; }
            if (!dbh.mysqlAvailable()) { reply(ex, 503, "MYSQL_REQUIRED"); return; }

            String q = ex.getRequestURI().getQuery();
            String u = queryParam(q, "username");
            String p = queryParam(q, "password");
            if (u == null || p == null || u.isBlank() || p.isBlank()) {
                reply(ex, 400, "MISSING username/password");
                return;
            }

            try {
                UserDao.AuthRow auth;
                try (var mysql = Db.mysql()) { auth = userDao.getAuth(mysql, u); }

                if (auth == null || !PasswordHasher.verify(p.toCharArray(), auth.passwordHash())) {
                    try (var mysql = Db.mysql()) {
                        auditDao.log(mysql, u, "LOGIN", "INVALID", false, remote(ex));
                    } catch (Exception ignored) {}
                    reply(ex, 401, "INVALID_LOGIN");
                    return;
                }

                String token;
                try (var sqlite = Db.sqlite()) { token = sessionDao.newSession(sqlite, u); }

                String role = auth.role() == null ? "STANDARD" : auth.role();

                try (var mysql = Db.mysql()) {
                    auditDao.log(mysql, u, "LOGIN", "", true, remote(ex));
                } catch (Exception ignored) {}

                reply(ex, 200, "TOKEN " + token + " ROLE " + role);
            } catch (Exception e) {
                reply(ex, 500, "AUTH_ERROR " + e.getMessage());
            }
        });

        // --- FILE LIST / TERM / PING ---

        server.createContext("/api/files", ex -> {
            if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) { reply(ex, 405, "METHOD_NOT_ALLOWED"); return; }
            String username = requireUser(ex, sessionDao);
            if (username == null) return;

            try (var sqlite = Db.sqlite()) {
                var ids = cacheDao.listFileIds(sqlite);
                reply(ex, 200, String.join("\n", ids));
            } catch (Exception e) {
                reply(ex, 500, "SQLITE_ERROR " + e.getMessage());
            }
        });

        server.createContext("/api/term", ex -> {
            if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) { reply(ex, 405, "METHOD_NOT_ALLOWED"); return; }

            String username = requireUser(ex, sessionDao);
            if (username == null) return;

            String q = ex.getRequestURI().getQuery();
            String cmd = queryParam(q, "cmd");
            if (cmd == null || cmd.isBlank()) { reply(ex, 400, "MISSING cmd"); return; }

            try (var sqlite = Db.sqlite()) {
                String cwd = cwdDao.getOrInit(sqlite, username);

                String out = handleCmd(
                        username, cwd, cmd,
                        cwdDao, vfs, aclDao, nanoDao,
                        manifests, placement,
                        jobQueue, fileLocks, registry, scheduler, replicas, proxy, crypto,
                        fileDao, cacheDao, dbh, auditDao,
                        sqlite,
                        remote(ex)
                );

                reply(ex, 200, out);
            } catch (Exception e) {
                reply(ex, 500, "TERM_ERROR " + e.getMessage());
            }
        });

        server.createContext("/api/ping", ex -> {
            try {
                var fut = jobQueue.submit(() -> {
                    var pick = scheduler.pick(registry.healthy());
                    if (pick.isEmpty()) return new Result(503, "NO_HEALTHY_NODES", null);

                    var node = pick.get();
                    long start = System.nanoTime();
                    node.incInFlight();
                    try {
                        String body = proxy.get(node.baseUrl() + "/ping", 120);
                        long tookMs = (System.nanoTime() - start) / 1_000_000L;
                        node.recordLatencyMs(tookMs);
                        return new Result(200, "LB(" + scheduler.name() + ") -> " + node.nodeId()
                                + " in " + tookMs + "ms | " + body, null);
                    } catch (Exception e) {
                        node.setHealthy(false);
                        return new Result(502, "UPSTREAM_FAIL " + node.nodeId() + " " + e.getMessage(), null);
                    } finally {
                        node.decInFlight();
                    }
                });

                Result r = fut.get();
                reply(ex, r.code, r.body);
            } catch (java.util.concurrent.RejectedExecutionException rej) {
                reply(ex, 429, "TOO_BUSY queue=" + jobQueue.queued() + "/" + jobQueue.capacity());
            } catch (Exception e) {
                reply(ex, 500, "LB_ERROR " + e.getMessage());
            }
        });

        // --- ACL ---

        server.createContext("/api/acl/grant", ex -> {
            if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) { reply(ex, 405, "METHOD_NOT_ALLOWED"); return; }
            if (!dbh.mysqlAvailable()) { reply(ex, 503, "MYSQL_REQUIRED"); return; }

            String username = requireUser(ex, sessionDao);
            if (username == null) return;

            String q = ex.getRequestURI().getQuery();
            String fileId = queryParam(q, "fileId");
            String target = queryParam(q, "target");
            if (fileId == null || target == null) { reply(ex, 400, "MISSING fileId/target"); return; }

            boolean permRead = parseBool(queryParam(q, "read"), true);
            boolean permWrite = parseBool(queryParam(q, "write"), false);

            try (var mysql = Db.mysql()) {
                if (!aclDao.canWrite(mysql, fileId, username)) {
                    auditDao.log(mysql, username, "ACL_GRANT", "file=" + fileId + " target=" + target, false, remote(ex));
                    reply(ex, 403, "FORBIDDEN");
                    return;
                }
                aclDao.grant(mysql, fileId, target, permRead, permWrite);
                auditDao.log(mysql, username, "ACL_GRANT",
                        "file=" + fileId + " target=" + target + " read=" + permRead + " write=" + permWrite,
                        true, remote(ex));
                reply(ex, 200, "GRANTED fileId=" + fileId + " to=" + target + " read=" + permRead + " write=" + permWrite);
            } catch (Exception e) {
                reply(ex, 500, "ACL_ERROR " + e.getMessage());
            }
        });

        server.createContext("/api/acl/revoke", ex -> {
            if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) { reply(ex, 405, "METHOD_NOT_ALLOWED"); return; }
            if (!dbh.mysqlAvailable()) { reply(ex, 503, "MYSQL_REQUIRED"); return; }

            String username = requireUser(ex, sessionDao);
            if (username == null) return;

            String q = ex.getRequestURI().getQuery();
            String fileId = queryParam(q, "fileId");
            String target = queryParam(q, "target");
            if (fileId == null || target == null) { reply(ex, 400, "MISSING fileId/target"); return; }

            try (var mysql = Db.mysql()) {
                if (!aclDao.canWrite(mysql, fileId, username)) {
                    auditDao.log(mysql, username, "ACL_REVOKE", "file=" + fileId + " target=" + target, false, remote(ex));
                    reply(ex, 403, "FORBIDDEN");
                    return;
                }
                aclDao.revoke(mysql, fileId, target);
                auditDao.log(mysql, username, "ACL_REVOKE", "file=" + fileId + " target=" + target, true, remote(ex));
                reply(ex, 200, "REVOKED fileId=" + fileId + " from=" + target);
            } catch (Exception e) {
                reply(ex, 500, "ACL_ERROR " + e.getMessage());
            }
        });

        // --- ADMIN ---

        server.createContext("/api/admin/users", ex -> {
            if ("GET".equalsIgnoreCase(ex.getRequestMethod())) {
                String admin = requireAdmin(ex, sessionDao, userDao);
                if (admin == null) return;

                try (var mysql = Db.mysql()) {
                    var rows = userDao.listUsers(mysql);
                    StringBuilder sb = new StringBuilder();
                    for (var r : rows) {
                        sb.append(r.username()).append(" ").append(r.role()).append(" ").append(r.createdAt()).append("\n");
                    }
                    reply(ex, 200, sb.toString().stripTrailing());
                } catch (Exception e) {
                    reply(ex, 500, "ADMIN_LIST_ERROR " + e.getMessage());
                }
                return;
            }

            if ("DELETE".equalsIgnoreCase(ex.getRequestMethod())) {
                String admin = requireAdmin(ex, sessionDao, userDao);
                if (admin == null) return;

                String q = ex.getRequestURI().getQuery();
                String u = queryParam(q, "username");
                if (u == null || u.isBlank()) { reply(ex, 400, "MISSING username"); return; }

                try (var mysql = Db.mysql()) {
                    boolean ok = userDao.deleteUser(mysql, u);
                    auditDao.log(mysql, admin, "ADMIN_DELETE", u, ok, remote(ex));
                    reply(ex, ok ? 200 : 404, ok ? "DELETED" : "NOT_FOUND");
                } catch (Exception e) {
                    reply(ex, 500, "ADMIN_DELETE_ERROR " + e.getMessage());
                }
                return;
            }

            reply(ex, 405, "METHOD_NOT_ALLOWED");
        });

        server.createContext("/api/admin/create", ex -> {
            if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) { reply(ex, 405, "METHOD_NOT_ALLOWED"); return; }
            String admin = requireAdmin(ex, sessionDao, userDao);
            if (admin == null) return;

            String q = ex.getRequestURI().getQuery();
            String u = queryParam(q, "username");
            String p = queryParam(q, "password");
            String role = queryParam(q, "role");
            if (u == null || p == null) { reply(ex, 400, "MISSING username/password"); return; }

            String hash = PasswordHasher.hash(p.toCharArray());
            try (var mysql = Db.mysql()) {
                boolean ok = userDao.createUser(mysql, u, hash, role == null ? "STANDARD" : role);
                auditDao.log(mysql, admin, "ADMIN_CREATE", u + " role=" + (role == null ? "STANDARD" : role), ok, remote(ex));
                reply(ex, ok ? 200 : 409, ok ? "CREATED" : "USERNAME_TAKEN");
            } catch (Exception e) {
                reply(ex, 500, "ADMIN_CREATE_ERROR " + e.getMessage());
            }
        });

        server.createContext("/api/admin/role", ex -> {
            if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) { reply(ex, 405, "METHOD_NOT_ALLOWED"); return; }
            String admin = requireAdmin(ex, sessionDao, userDao);
            if (admin == null) return;

            String q = ex.getRequestURI().getQuery();
            String u = queryParam(q, "username");
            String role = queryParam(q, "role");
            if (u == null || role == null) { reply(ex, 400, "MISSING username/role"); return; }

            try (var mysql = Db.mysql()) {
                boolean ok = userDao.setRole(mysql, u, role);
                auditDao.log(mysql, admin, "ADMIN_ROLE", u + " role=" + role, ok, remote(ex));
                reply(ex, ok ? 200 : 404, ok ? "UPDATED" : "NOT_FOUND");
            } catch (Exception e) {
                reply(ex, 500, "ADMIN_ROLE_ERROR " + e.getMessage());
            }
        });

        server.createContext("/api/admin/audit", ex -> {
            if (!"GET".equalsIgnoreCase(ex.getRequestMethod())) { reply(ex, 405, "METHOD_NOT_ALLOWED"); return; }
            String admin = requireAdmin(ex, sessionDao, userDao);
            if (admin == null) return;

            int limit = 200;
            try {
                String q = ex.getRequestURI().getQuery();
                String lim = queryParam(q, "limit");
                if (lim != null) limit = Integer.parseInt(lim.trim());
            } catch (Exception ignored) {}

            try (var mysql = Db.mysql()) {
                var lines = auditDao.latest(mysql, limit);
                reply(ex, 200, String.join("\n", lines));
            } catch (Exception e) {
                reply(ex, 500, "AUDIT_ERROR " + e.getMessage());
            }
        });

        // --- DATA: chunk + file ---

        server.createContext("/api/chunk", ex -> {
            if (!dbh.mysqlAvailable()) { reply(ex, 503, "MYSQL_REQUIRED"); return; }

            String username = requireUser(ex, sessionDao);
            if (username == null) return;

            String q = ex.getRequestURI().getQuery();
            String fileId = queryParam(q, "fileId");
            String chunkId = queryParam(q, "chunkId");
            if (fileId == null || chunkId == null) { reply(ex, 400, "MISSING fileId/chunkId"); return; }

            String method = ex.getRequestMethod().toUpperCase();

            try {
                if ("PUT".equals(method)) {
                    if (!canWrite(aclDao, fileId, username)) { reply(ex, 403, "FORBIDDEN"); return; }
                    byte[] plain = ex.getRequestBody().readAllBytes();

                    var fut = jobQueue.submit(() ->
                            fileLocks.withWrite(fileId, () -> {
                                var targets = ReplicaPicker.pick(registry.healthy(), scheduler, replicas);
                                if (targets.isEmpty()) return new Result(503, "NO_HEALTHY_NODES", null);
                                byte[] cipher = crypto.encrypt(fileId, chunkId, plain);
                                long crcPlain = crc32(plain);

                                List<String> storedOn = new ArrayList<>();
                                for (var node : targets) {
                                    node.incInFlight();
                                    try {
                                        proxy.putBytes(node.baseUrl() + "/chunk?fileId=" + fileId + "&chunkId=" + chunkId, cipher, 180);
                                        storedOn.add(node.baseUrl());
                                    } catch (Exception e) {
                                        node.setHealthy(false);
                                    } finally {
                                        node.decInFlight();
                                    }
                                }

                                if (storedOn.isEmpty()) return new Result(502, "ALL_REPLICAS_FAILED", null);

                                placement.put(fileId, chunkId, new ChunkPlacement.Entry(List.copyOf(storedOn), crcPlain, plain.length));
                                return new Result(200, "STORED replicas=" + storedOn.size(), null);
                            })
                    );

                    Result r = fut.get();
                    reply(ex, r.code, r.body);
                    return;
                }

                if ("GET".equals(method)) {
                    if (!canRead(aclDao, fileId, username)) { reply(ex, 403, "FORBIDDEN"); return; }

                    var fut = jobQueue.submit(() ->
                            fileLocks.withRead(fileId, () -> {
                                var entry = placement.get(fileId, chunkId);
                                if (entry == null) return new Result(404, "UNKNOWN_CHUNK", null);

                                for (String nodeUrl : entry.nodeBaseUrls()) {
                                    try {
                                        byte[] cipher = proxy.getBytes(nodeUrl + "/chunk?fileId=" + fileId + "&chunkId=" + chunkId, 180);
                                        byte[] plain = crypto.decrypt(fileId, chunkId, cipher);
                                        if (entry.crc32() != 0L && crc32(plain) != entry.crc32()) continue;
                                        return new Result(200, null, plain);
                                    } catch (Exception ignored) {}
                                }
                                return new Result(404, "NOT_FOUND_ON_ALL_REPLICAS", null);
                            })
                    );

                    Result r = fut.get();
                    if (r.bytes != null) {
                        ex.getResponseHeaders().set("Content-Type", "application/octet-stream");
                        ex.sendResponseHeaders(200, r.bytes.length);
                        try (var os = ex.getResponseBody()) { os.write(r.bytes); }
                    } else {
                        reply(ex, r.code, r.body);
                    }
                    return;
                }

                reply(ex, 405, "METHOD_NOT_ALLOWED");
            } catch (IOException io) {
                reply(ex, 400, "BAD_BODY");
            } catch (java.util.concurrent.RejectedExecutionException rej) {
                reply(ex, 429, "TOO_BUSY queue=" + jobQueue.queued() + "/" + jobQueue.capacity());
            } catch (Exception e) {
                reply(ex, 500, "LB_ERROR " + e.getMessage());
            }
        });

        server.createContext("/api/file", ex -> {
            if (!dbh.mysqlAvailable()) { reply(ex, 503, "MYSQL_REQUIRED"); return; }

            String username = requireUser(ex, sessionDao);
            if (username == null) return;

            String q = ex.getRequestURI().getQuery();
            String fileId = queryParam(q, "fileId");
            if (fileId == null || fileId.isBlank()) { reply(ex, 400, "MISSING fileId"); return; }

            String method = ex.getRequestMethod().toUpperCase();

            try {
                if ("PUT".equals(method)) {
                    if (!canWrite(aclDao, fileId, username)) { reply(ex, 403, "FORBIDDEN"); return; }
                    byte[] bodyBytes = ex.getRequestBody().readAllBytes();

                    int chunkSizeTmp = 1024 * 1024;
                    String cs = queryParam(q, "chunkSize");
                    if (cs != null) {
                        try {
                            int parsed = Integer.parseInt(cs.trim());
                            if (parsed >= 4096 && parsed <= 4 * 1024 * 1024) chunkSizeTmp = parsed;
                        } catch (NumberFormatException ignored) {}
                    }
                    final int chunkSize = chunkSizeTmp;

                    var fut = jobQueue.submit(() ->
                            storeBytesToFileId(
                                    username, fileId, bodyBytes, chunkSize,
                                    fileLocks, registry, scheduler, replicas, proxy, crypto,
                                    manifests, placement, cacheDao, fileDao, aclDao
                            )
                    );

                    Result r = fut.get();

                    if (dbh.mysqlAvailable()) {
                        try (var mysql = Db.mysql()) {
                            auditDao.log(mysql, username, "UPLOAD", "file=" + fileId + " result=" + r.code, r.code == 200, remote(ex));
                        } catch (Exception ignored) {}
                    }

                    reply(ex, r.code, r.body);
                    return;
                }

                if ("GET".equals(method)) {
                    if (!canRead(aclDao, fileId, username)) { reply(ex, 403, "FORBIDDEN"); return; }

                    var fut = jobQueue.submit(() ->
                            fileLocks.withRead(fileId, () -> {
                                var m = manifests.get(fileId);
                                if (m == null) return new Result(404, "UNKNOWN_FILE", null);

                                var out = new java.io.ByteArrayOutputStream();
                                for (String chunkId : m.chunkIds()) {
                                    var entry = placement.get(fileId, chunkId);
                                    if (entry == null) return new Result(404, "MISSING_CHUNK " + chunkId, null);

                                    byte[] plain = null;
                                    for (String nodeUrl : entry.nodeBaseUrls()) {
                                        try {
                                            byte[] cipher = proxy.getBytes(nodeUrl + "/chunk?fileId=" + fileId + "&chunkId=" + chunkId, 300);
                                            byte[] dec = crypto.decrypt(fileId, chunkId, cipher);
                                            if (entry.crc32() != 0L && crc32(dec) != entry.crc32()) continue;
                                            plain = dec;
                                            break;
                                        } catch (Exception ignored) {}
                                    }
                                    if (plain == null) return new Result(404, "CHUNK_UNAVAILABLE " + chunkId, null);
                                    out.writeBytes(plain);
                                }

                                return new Result(200, null, out.toByteArray());
                            })
                    );

                    Result r = fut.get();

                    if (dbh.mysqlAvailable()) {
                        try (var mysql = Db.mysql()) {
                            auditDao.log(mysql, username, "DOWNLOAD", "file=" + fileId + " result=" + r.code, r.code == 200, remote(ex));
                        } catch (Exception ignored) {}
                    }

                    if (r.bytes != null) {
                        ex.getResponseHeaders().set("Content-Type", "application/octet-stream");
                        ex.sendResponseHeaders(200, r.bytes.length);
                        try (var os = ex.getResponseBody()) { os.write(r.bytes); }
                    } else {
                        reply(ex, r.code, r.body);
                    }
                    return;
                }

                if ("DELETE".equals(method)) {
                    if (!canWrite(aclDao, fileId, username)) { reply(ex, 403, "FORBIDDEN"); return; }

                    try {
                        var fut = jobQueue.submit(() -> {
                            var m = manifests.get(fileId);

                            if (m != null) {
                                for (String chunkId : m.chunkIds()) placement.remove(fileId, chunkId);
                                manifests.remove(fileId);

                                for (NodeInfo n : registry.all()) {
                                    try { proxy.delete(n.baseUrl() + "/file?fileId=" + fileId, 60); }
                                    catch (Exception ignored) {}
                                }
                            }

                            try (var sqlite = Db.sqlite()) {
                                cacheDao.deleteCachedFile(sqlite, fileId);
                            } catch (Exception ignored) {}

                            if (dbh.mysqlAvailable()) {
                                try (var mysql = Db.mysql()) {
                                    fileDao.deleteFile(mysql, fileId);
                                    aclDao.deleteAllForFile(mysql, fileId);
                                } catch (Exception ignored) {}
                            }

                            return new Result(200, "DELETED fileId=" + fileId, null);
                        });

                        Result r = fut.get();

                        if (dbh.mysqlAvailable()) {
                            try (var mysql = Db.mysql()) {
                                auditDao.log(mysql, username, "DELETE", "file=" + fileId + " result=" + r.code, r.code == 200, remote(ex));
                            } catch (Exception ignored) {}
                        }

                        reply(ex, r.code, r.body);
                    } catch (Exception e) {
                        reply(ex, 500, "LB_ERROR " + e.getMessage());
                    }
                    return;
                }

                reply(ex, 405, "METHOD_NOT_ALLOWED");
            } catch (IOException io) {
                reply(ex, 400, "BAD_BODY");
            } catch (java.util.concurrent.RejectedExecutionException rej) {
                reply(ex, 429, "TOO_BUSY queue=" + jobQueue.queued() + "/" + jobQueue.capacity());
            } catch (Exception e) {
                reply(ex, 500, "LB_ERROR " + e.getMessage());
            }
        });

        server.setExecutor(null);
        server.start();

        log.info("LoadBalancer started on port {} (build={})", port, BUILD_TAG);
        log.info("Scheduler: {}", scheduler.name());
        log.info("Configured nodes: {}", registry.all().size());
        log.info("Queue: workers={} capacity={}", workers, qcap);
    }

    // ----------------- TERM -----------------

    private static String handleCmd(
            String user, String cwd, String cmdLine,
            CwdDao cwdDao,
            VfsIndex vfs,
            AclDao aclDao,
            NanoDao nanoDao,
            FileManifestStore manifests,
            ChunkPlacement placement,
            JobQueue jobQueue,
            FileLocks fileLocks,
            NodeRegistry registry,
            Scheduler scheduler,
            int replicas,
            ProxyClient proxy,
            Crypto crypto,
            FileDao fileDao,
            SqliteCacheDao cacheDao,
            DbBootstrap.DbHandles dbh,
            AuditDao auditDao,
            Connection sqlite,
            String remoteIp
    ) throws Exception {

        String[] parts = cmdLine.split("\\s+");
        String cmd = parts[0];

        switch (cmd) {
            case "whoami": return user;
            case "pwd": return cwd;

            case "cd": {
                String target = (parts.length >= 2) ? parts[1] : "/";
                String newPath = normalizePath(cwd, target);

                if (!"/".equals(newPath)) {
                    var e = vfs.get(user, newPath);
                    if (e == null || !e.isDir()) return "cd: no such directory: " + target;
                }

                cwdDao.set(sqlite, user, newPath);
                return "";
            }

            case "mkdir": {
                if (parts.length < 2) return "mkdir: missing operand";
                vfs.mkdir(user, normalizePath(cwd, parts[1]));
                return "";
            }

            case "ls": {
                String dir = (parts.length >= 2) ? normalizePath(cwd, parts[1]) : cwd;
                var kids = vfs.listChildren(user, dir);
                if (kids.isEmpty()) return "";
                return String.join("\n", kids);
            }

            case "tree": {
                String dir = (parts.length >= 2) ? normalizePath(cwd, parts[1]) : cwd;
                String t = tree(user, dir, vfs, 0);
                return t == null ? "" : t;
            }

            case "ps":{
                return "PID CMD\n1 lb\n2 healthchecker\n3 workers";


            }
            
            case "rm": {
                if (parts.length < 2) return "usage: rm <path>";
                String p = normalizePath(cwd, parts[1]);

                var e = vfs.get(user, p);
                if (e == null) return "rm: no such file or directory: " + p;
                if (e.isDir()) return "rm: is a directory: " + p + " (not supported)";

                String fileId = e.fileId();
                if (!canWrite(aclDao, fileId, user)) return "rm: permission denied: " + p;

                // remove user's VFS link first (fast)
                try {
                    vfs.rm(user, p);
                } catch (Exception ex) {
                    return "rm: error: " + ex.getMessage();
                }

                // then delete underlying file bytes + metadata (best effort)
                try {
                    Result r = deleteFileById(
                            fileId,
                            manifests, placement, registry, proxy, cacheDao, fileDao, aclDao, dbh
                    );

                    if (dbh.mysqlAvailable()) {
                        try (var mysql = Db.mysql()) {
                            auditDao.log(mysql, user, "RM", "path=" + p + " file=" + fileId + " result=" + r.code, r.code == 200, remoteIp);
                        } catch (Exception ignored) {}
                    }

                    return r.code == 200 ? "" : ("rm: " + r.body);
                } catch (Exception ex) {
                    return "rm: error: " + ex.getMessage();
                }
            }

            
            case "touch": {
                if (parts.length < 2) return "usage: touch <path>";
                String p = normalizePath(cwd, parts[1]);

                // if already exists, do nothing (like real touch)
                var existing = vfs.get(user, p);
                if (existing != null) return "";

                String fileId = java.util.UUID.randomUUID().toString();

                try {
                    vfs.touch(user, p, fileId);

                bootstrapNewFile(user, fileId, manifests, cacheDao, fileDao, aclDao);
                return "";
                } catch (Exception e) {
                    return "touch: error: " + e.getMessage();
                }
            }



            case "cat": {
                if (parts.length < 2) return "usage: cat <path>";
                String p = normalizePath(cwd, parts[1]);
                try {
                    return readTextFile(user, p, vfs, aclDao, manifests, placement, proxy, crypto);
                } catch (Exception e) {
                    return e.getMessage();
                }
            }

            case "share": {
                if (parts.length < 4) return "usage: share <path> <user> r|rw";

                String p = normalizePath(cwd, parts[1]);
                String targetUser = parts[2];
                String mode = parts[3].toLowerCase();

                boolean r = mode.startsWith("r");
                boolean w = mode.contains("w");

                var e = vfs.get(user, p);
                if (e == null || e.isDir()) return "share: not a file: " + p;

                String fileId = e.fileId();

                // only owner/writer can share
                if (!canWrite(aclDao, fileId, user)) return "share: permission denied: " + p;

                try (var mysql = Db.mysql()) {
                    aclDao.grant(mysql, fileId, targetUser, r, w);
                } catch (Exception ex) {
                    return "share: error: " + ex.getMessage();
                }

                // CRITICAL: link path into target user's VFS so cat/nano finds it
                try {
                    vfs.linkFile(targetUser, p, fileId);
                } catch (Exception ex) {
                    return "share: granted but link failed: " + ex.getMessage();
                }

                return "shared " + p + " with " + targetUser + " " + (w ? "rw" : "r");
            }

            case "unshare": {
                if (parts.length < 3) return "usage: unshare <path> <user>";
                String p = normalizePath(cwd, parts[1]);
                String target = parts[2];

                var e = vfs.get(user, p);
                if (e == null || e.isDir()) return "unshare: not a file: " + p;

                String fileId = e.fileId();
                if (!canWrite(aclDao, fileId, user)) return "unshare: permission denied: " + p;

                try (var mysql = Db.mysql()) {
                    aclDao.revoke(mysql, fileId, target);
                } catch (Exception ex) {
                    return "unshare: error: " + ex.getMessage();
                }

                // remove target user's VFS link (so their namespace is clean)
                try {
                    if (vfs.exists(target, p)) vfs.rm(target, p);
                } catch (Exception ex) {
                    // ACL revoked is the important part; keep going but report it
                    return "unshared " + p + " from " + target + " (link cleanup failed: " + ex.getMessage() + ")";
                }

                if (dbh.mysqlAvailable()) {
                    try (var mysql = Db.mysql()) {
                        auditDao.log(mysql, user, "UNSHARE", "path=" + p + " file=" + fileId + " target=" + target, true, remoteIp);
                    } catch (Exception ignored) {}
                }

                return "unshared " + p + " from " + target;
            }

            case "nano": {
                if (parts.length < 2) return "usage: nano <path>";
                String p = normalizePath(cwd, parts[1]);

                // create file if missing
                var e = vfs.get(user, p);
                if (e == null) {
                    String fileId = java.util.UUID.randomUUID().toString();
                    vfs.touch(user, p, fileId);
                    bootstrapNewFile(user, fileId, manifests, cacheDao, fileDao, aclDao);
                    try (var mysql = Db.mysql()) {
                        aclDao.ensureOwnerFullAccess(mysql, fileId, user);
                    }

                    manifests.put(fileId, new FileManifestStore.Manifest(fileId, 0L, 0, List.of()));
                }

                String initial;
                try {
                    initial = readTextFile(user, p, vfs, aclDao, manifests, placement, proxy, crypto);
                } catch (Exception ignored) {
                    initial = "";
                }

                nanoDao.open(sqlite, user, cwd, p, initial);

                return "nano: editing " + p + "\n"
                        + "commands: nano.show | nano.set <text> | nano.append <text> | nano.save | nano.exit";
            }

            case "nano.show": {
                var r = nanoDao.get(sqlite, user);
                if (r == null) return "nano: no active session";
                return r.buffer() == null ? "" : r.buffer();
            }

            case "nano.set": {
                String text = "";
                if (parts.length >= 2) text = cmdLine.substring(cmdLine.indexOf(' ') + 1);
                var r = nanoDao.get(sqlite, user);
                if (r == null) return "nano: no active session";
                nanoDao.setBuffer(sqlite, user, text);
                return "nano: buffer set";
            }

            case "nano.append": {
                String text = "";
                if (parts.length >= 2) text = cmdLine.substring(cmdLine.indexOf(' ') + 1);
                var r = nanoDao.get(sqlite, user);
                if (r == null) return "nano: no active session";
                nanoDao.appendLine(sqlite, user, text);
                return "nano: appended";
            }

            case "nano.exit": {
                nanoDao.close(sqlite, user);
                return "nano: session closed";
            }

            case "nano.save": {
                var s = nanoDao.get(sqlite, user);
                if (s == null) return "nano: no active session";

                var e = vfs.get(user, s.path());
                if (e == null || e.isDir()) return "nano: not a file: " + s.path();

                String fileId = e.fileId();
                if (!canWrite(aclDao, fileId, user)) return "nano: permission denied: " + s.path();

                byte[] bytes = (s.buffer() == null ? "" : s.buffer()).getBytes(StandardCharsets.UTF_8);

                // IMPORTANT: do not enqueue / block on jobQueue here (terminal must be responsive)
                Result r = storeBytesToFileId(
                        user, fileId, bytes, 64 * 1024,
                        fileLocks, registry, scheduler, replicas, proxy, crypto,
                        manifests, placement, cacheDao, fileDao, aclDao
                );

                if (r.code != 200) return "nano: error: " + r.body;

                if (dbh.mysqlAvailable()) {
                    try (var mysql = Db.mysql()) {
                        auditDao.log(mysql, user, "NANO_SAVE", "path=" + s.path() + " file=" + fileId, true, remoteIp);
                    } catch (Exception ignored) {}
                }

                return "nano: saved " + s.path();
            }

            default:
                return "unknown command: " + cmd;
        }
    }

    // ----------------- STORE/READ HELPERS -----------------

    private static Result storeBytesToFileId(
            String username,
            String fileId,
            byte[] bodyBytes,
            int chunkSize,
            FileLocks fileLocks,
            NodeRegistry registry,
            Scheduler scheduler,
            int replicas,
            ProxyClient proxy,
            Crypto crypto,
            FileManifestStore manifests,
            ChunkPlacement placement,
            SqliteCacheDao cacheDao,
            FileDao fileDao,
            AclDao aclDao
    ) {

        try {
            return fileLocks.withWrite(fileId, () -> {
                var chunkIds = new ArrayList<String>();
                long total = 0L;

                try (var in = new java.io.ByteArrayInputStream(bodyBytes)) {
                    int idx = 0;
                    while (true) {
                        byte[] plainChunk = Chunker.nextChunk(in, chunkSize);
                        if (plainChunk == null) break;

                        String chunkId = "c" + idx++;
                        chunkIds.add(chunkId);
                        total += plainChunk.length;

                        var targets = ReplicaPicker.pick(registry.healthy(), scheduler, replicas);
                        if (targets.isEmpty()) return new Result(503, "NO_HEALTHY_NODES", null);

                        byte[] cipherChunk = crypto.encrypt(fileId, chunkId, plainChunk);
                        long crcPlain = crc32(plainChunk);

                        List<String> storedOn = new ArrayList<>();
                        for (var node : targets) {
                            node.incInFlight();
                            try {
                                proxy.putBytes(node.baseUrl() + "/chunk?fileId=" + fileId + "&chunkId=" + chunkId, cipherChunk, 300);
                                storedOn.add(node.baseUrl());
                            } catch (Exception e) {
                                node.setHealthy(false);
                            } finally {
                                node.decInFlight();
                            }
                        }

                        if (storedOn.isEmpty()) return new Result(502, "ALL_REPLICAS_FAILED chunk=" + chunkId, null);

                        placement.put(fileId, chunkId,
                                new ChunkPlacement.Entry(List.copyOf(storedOn), crcPlain, plainChunk.length));
                    }
                } catch (IOException io) {
                    return new Result(500, "CHUNK_READ_ERROR", null);
                }

                var manifest = new FileManifestStore.Manifest(fileId, total, chunkSize, List.copyOf(chunkIds));
                manifests.put(fileId, manifest);

                try (var sqlite = Db.sqlite()) {
                    cacheDao.upsertCachedFile(sqlite, manifest);
                } catch (Exception ignored) {}

                try (var mysql = Db.mysql()) {
                    fileDao.upsertFile(mysql, manifest);
                    for (String cid : manifest.chunkIds()) {
                        var entry = placement.get(fileId, cid);
                        if (entry != null) fileDao.upsertPlacement(mysql, fileId, cid, entry);
                    }
                    aclDao.ensureOwnerFullAccess(mysql, fileId, username);
                } catch (Exception e) {
                    log.warn("MySQL persist failed (upload kept): {}", e.getMessage());
                }

                return new Result(200, "UPLOADED fileId=" + fileId + " bytes=" + total + " chunks=" + chunkIds.size(), null);
            });
        } catch (Exception e) {
            return new Result(500, "STORE_ERROR " + e.getMessage(), null);
        }
    }

        private static Result deleteFileById(
                String fileId,
                FileManifestStore manifests,
                ChunkPlacement placement,
                NodeRegistry registry,
                ProxyClient proxy,
                SqliteCacheDao cacheDao,
                FileDao fileDao,
                AclDao aclDao,
                DbBootstrap.DbHandles dbh
        ) {
            try {
                var m = manifests.get(fileId);

                if (m != null) {
                    for (String chunkId : m.chunkIds()) placement.remove(fileId, chunkId);
                    manifests.remove(fileId);

                    // best-effort: ask storage nodes to delete their local file copies
                    for (NodeInfo n : registry.all()) {
                        try { proxy.delete(n.baseUrl() + "/file?fileId=" + fileId, 60); }
                        catch (Exception ignored) {}
                    }
                }

                try (var sqlite = Db.sqlite()) {
                    cacheDao.deleteCachedFile(sqlite, fileId);
                } catch (Exception ignored) {}

                if (dbh.mysqlAvailable()) {
                    try (var mysql = Db.mysql()) {
                        fileDao.deleteFile(mysql, fileId);
                        aclDao.deleteAllForFile(mysql, fileId);
                    } catch (Exception ignored) {}
                }

                return new Result(200, "DELETED fileId=" + fileId, null);
            } catch (Exception e) {
                return new Result(500, "DELETE_ERROR " + e.getMessage(), null);
            }
        }

        private static String readTextFile(
                String username,
                String path,
                VfsIndex vfs,
                AclDao aclDao,
                FileManifestStore manifests,
                ChunkPlacement placement,
                ProxyClient proxy,
                Crypto crypto
        ) throws Exception {

        var e = vfs.get(username, path);
        if (e == null || e.isDir()) throw new IllegalArgumentException("cat: not a file: " + path);

        String fileId = e.fileId();
        if (!canRead(aclDao, fileId, username))
            throw new IllegalArgumentException("cat: permission denied: " + path);

        var manifest = manifests.get(fileId);
        if (manifest == null) throw new IllegalArgumentException("cat: file missing");

        StringBuilder out = new StringBuilder();
        for (String chunkId : manifest.chunkIds()) {
            var pl = placement.get(fileId, chunkId);
            if (pl == null || pl.nodeBaseUrls().isEmpty())
                throw new IllegalArgumentException("cat: chunk missing");

            byte[] plain = null;
            for (String nodeUrl : pl.nodeBaseUrls()) {
                try {
                    byte[] cipher = proxy.getBytes(nodeUrl + "/chunk?fileId=" + fileId + "&chunkId=" + chunkId, 120);
                    byte[] dec = crypto.decrypt(fileId, chunkId, cipher);
                    plain = dec;
                    break;
                } catch (Exception ignored) {}
            }
            if (plain == null) throw new IllegalArgumentException("cat: chunk unavailable: " + chunkId);

            out.append(new String(plain, StandardCharsets.UTF_8));
        }
        return out.toString();
    }

    // ----------------- AUTH HELPERS -----------------

    private static String remote(HttpExchange ex) {
        try { return ex.getRemoteAddress().getAddress().getHostAddress(); }
        catch (Exception e) { return "unknown"; }
    }

    private static String requireUser(HttpExchange ex, SessionDao sessionDao) throws IOException {
        String token = bearerToken(ex);
        if (token == null) { reply(ex, 401, "MISSING_TOKEN"); return null; }

        try (var sqlite = Db.sqlite()) {
            String username = sessionDao.usernameFor(sqlite, token);
            if (username == null) { reply(ex, 401, "INVALID_TOKEN"); return null; }
            return username;
        } catch (Exception e) {
            reply(ex, 500, "SESSION_ERROR");
            return null;
        }
    }

    private static String requireAdmin(HttpExchange ex, SessionDao sessionDao, UserDao userDao) throws IOException {
        String username = requireUser(ex, sessionDao);
        if (username == null) return null;

        if (!DbBootstrap.init().mysqlAvailable()) { reply(ex, 503, "MYSQL_REQUIRED"); return null; }

        try (var mysql = Db.mysql()) {
            String role = userDao.getRole(mysql, username);
            if (!"ADMIN".equalsIgnoreCase(role)) { reply(ex, 403, "ADMIN_ONLY"); return null; }
            return username;
        } catch (Exception e) {
            reply(ex, 500, "ROLE_ERROR");
            return null;
        }
    }

    private static void bootstrapNewFile(
            String username,
            String fileId,
            FileManifestStore manifests,
            SqliteCacheDao cacheDao,
            FileDao fileDao,
            AclDao aclDao
    ) {
        // empty manifest in memory so cat works immediately
        var m = new FileManifestStore.Manifest(fileId, 0L, 0, List.of());
        manifests.put(fileId, m);

        // cache (best effort)
        try (var sqlite = Db.sqlite()) {
            cacheDao.upsertCachedFile(sqlite, m);
        } catch (Exception ignored) {}

        // MySQL: MUST create file row first (FK), THEN ACL
        try (var mysql = Db.mysql()) {
            fileDao.upsertFile(mysql, m);
            aclDao.ensureOwnerFullAccess(mysql, fileId, username);
        } catch (Exception ignored) {}
    }

    private static boolean parseBool(String s, boolean def) {
        if (s == null) return def;
        return "true".equalsIgnoreCase(s) || "1".equals(s) || "yes".equalsIgnoreCase(s);
    }

    private static boolean canRead(AclDao aclDao, String fileId, String username) {
        try (var mysql = Db.mysql()) { return aclDao.canRead(mysql, fileId, username); }
        catch (Exception e) { return false; }
    }

    private static boolean canWrite(AclDao aclDao, String fileId, String username) {
        try (var mysql = Db.mysql()) { return aclDao.canWrite(mysql, fileId, username); }
        catch (Exception e) { return false; }
    }

    private static String bearerToken(HttpExchange ex) {
        String h = ex.getRequestHeaders().getFirst("Authorization");
        if (h == null) return null;
        if (h.startsWith("Bearer ")) return h.substring(7).trim();
        return null;
    }

    // ----------------- SMALL UTILS -----------------

    private static String normalizePath(String cwd, String in) {
        if (in.startsWith("/")) return clean(in);
        if (cwd.endsWith("/")) return clean(cwd + in);
        return clean(cwd + "/" + in);
    }

    private static String clean(String p) {
        String x = p.replaceAll("/+", "/");
        if (x.length() > 1 && x.endsWith("/")) x = x.substring(0, x.length() - 1);
        return x;
    }

    private static String tree(String user, String dir, VfsIndex vfs, int depth) {
        StringBuilder sb = new StringBuilder();
        var kids = vfs.listChildren(user, dir);
        for (String name : kids) {
            for (int d = 0; d < depth; d++) sb.append("  ");
            sb.append("- ").append(name).append("\n");
            String child = ("/".equals(dir) ? "/" + name : dir + "/" + name);
            var e = vfs.get(user, child);
            if (e != null && e.isDir()) sb.append(tree(user, child, vfs, depth + 1)).append("\n");
        }
        return sb.toString().stripTrailing();
    }

    private static int readIntEnv(String key, int fallback) {
        String v = System.getenv(key);
        if (v == null || v.isBlank()) return fallback;
        try { return Integer.parseInt(v.trim()); }
        catch (NumberFormatException e) { return fallback; }
    }

    private static String queryParam(String query, String key) {
        if (query == null) return null;
        for (String part : query.split("&")) {
            String[] kv = part.split("=", 2);
            if (kv.length == 2 && kv[0].equals(key)) return urlDecode(kv[1]);
        }
        return null;
    }

    private static String urlDecode(String s) {
        try { return URLDecoder.decode(s, StandardCharsets.UTF_8); }
        catch (Exception e) { return s; }
    }

    private static long crc32(byte[] data) {
        CRC32 crc = new CRC32();
        crc.update(data);
        return crc.getValue();
    }

    private static void reply(HttpExchange ex, int code, String body) throws IOException {
        if (body == null) body = "";
        byte[] data = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
        ex.sendResponseHeaders(code, data.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(data); }
    }

    private record Result(int code, String body, byte[] bytes) {}
}