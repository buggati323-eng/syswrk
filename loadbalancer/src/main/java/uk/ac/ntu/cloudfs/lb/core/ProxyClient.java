package uk.ac.ntu.cloudfs.lb.core;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import static java.net.http.HttpRequest.BodyPublishers;
import static java.net.http.HttpResponse.BodyHandlers;

public final class ProxyClient {
    private final HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(3))
            .build();

    public String get(String url, int timeoutSeconds) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(timeoutSeconds))
                .GET()
                .build();

        HttpResponse<String> resp = client.send(req, BodyHandlers.ofString());
        if (resp.statusCode() != 200) throw new RuntimeException("Upstream status " + resp.statusCode() + " " + resp.body());
        return resp.body();
    }

    public String delete(String url, int timeoutSeconds) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(timeoutSeconds))
                .DELETE()
                .build();

        HttpResponse<String> resp = client.send(req, BodyHandlers.ofString());
        if (resp.statusCode() != 200) throw new RuntimeException("Upstream status " + resp.statusCode() + " " + resp.body());
        return resp.body();
    }

    public String putBytes(String url, byte[] data, int timeoutSeconds) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(timeoutSeconds))
                .PUT(BodyPublishers.ofByteArray(data))
                .build();

        HttpResponse<String> resp = client.send(req, BodyHandlers.ofString());
        if (resp.statusCode() != 200) throw new RuntimeException("Upstream status " + resp.statusCode() + " " + resp.body());
        return resp.body();
    }

    public byte[] getBytes(String url, int timeoutSeconds) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(timeoutSeconds))
                .GET()
                .build();

        HttpResponse<byte[]> resp = client.send(req, BodyHandlers.ofByteArray());
        if (resp.statusCode() != 200) throw new RuntimeException("Upstream status " + resp.statusCode());
        return resp.body();
    }
}