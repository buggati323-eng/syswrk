package uk.ac.ntu.cloudfs.ui;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;

public final class ApiClient {
    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    private final String baseUrl;
    private String token; // Bearer

    public ApiClient(String baseUrl) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
    }

    public void setToken(String token) { this.token = token; }
    public String token() { return token; }

    private static boolean isAuthEndpoint(String pathAndQuery) {
        return pathAndQuery != null && pathAndQuery.startsWith("/api/auth/");
    }

    private HttpRequest.Builder req(String pathAndQuery) {
        HttpRequest.Builder b = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + pathAndQuery))
                .timeout(Duration.ofSeconds(300));

        // IMPORTANT: never send Authorization header to auth endpoints
        if (!isAuthEndpoint(pathAndQuery)) {
            if (token != null && !token.isBlank()) b.header("Authorization", "Bearer " + token);
        }

        return b;
    }

    private static void throwIfError(HttpResponse<String> r) {
        if (r.statusCode() >= 400) throw new RuntimeException(r.statusCode() + " " + (r.body() == null ? "" : r.body().trim()));
    }

    public String post(String pathAndQuery) throws Exception {
        HttpResponse<String> r = http.send(
                req(pathAndQuery).POST(HttpRequest.BodyPublishers.noBody()).build(),
                HttpResponse.BodyHandlers.ofString()
        );
        throwIfError(r);
        return r.body() == null ? "" : r.body();
    }

    public String delete(String pathAndQuery) throws Exception {
    HttpResponse<String> r = http.send(req(pathAndQuery).DELETE().build(),
            HttpResponse.BodyHandlers.ofString());
    if (r.statusCode() >= 400) throw new RuntimeException(r.statusCode() + " " + r.body());
    return r.body();
}

    public String getText(String pathAndQuery) throws Exception {
        HttpResponse<String> r = http.send(
                req(pathAndQuery).GET().build(),
                HttpResponse.BodyHandlers.ofString()
        );
        throwIfError(r);
        return r.body() == null ? "" : r.body();
    }

    public void putFile(String pathAndQuery, Path file) throws Exception {
        HttpResponse<String> r = http.send(
                req(pathAndQuery).PUT(HttpRequest.BodyPublishers.ofFile(file)).build(),
                HttpResponse.BodyHandlers.ofString()
        );
        throwIfError(r);
    }

    public void downloadTo(String pathAndQuery, Path out) throws Exception {
        HttpResponse<Path> r = http.send(
                req(pathAndQuery).GET().build(),
                HttpResponse.BodyHandlers.ofFile(out)
        );
        if (r.statusCode() >= 400) {
            String body = safeReadBodyAsText(pathAndQuery);
            throw new RuntimeException(r.statusCode() + " " + (body == null ? "" : body.trim()));
        }
    }

    private String safeReadBodyAsText(String pathAndQuery) {
        try {
            HttpResponse<String> r = http.send(
                    req(pathAndQuery).GET().build(),
                    HttpResponse.BodyHandlers.ofString()
            );
            return r.body();
        } catch (Exception ignored) {
            return "";
        }
    }

    public static String enc(String s) {
        return URLEncoder.encode(s == null ? "" : s, StandardCharsets.UTF_8);
    }
}