package uk.ac.ntu.cloudfs.ui;

public final class Session {
    private static ApiClient api;
    private static String username;
    private static String role;   // "ADMIN" or "STANDARD"
    private static String baseUrl;

    private Session() {}

    public static void clear() {
        api = null;
        username = null;
        role = null;
        baseUrl = null;
    }

    public static ApiClient api() { return api; }
    public static void setApi(ApiClient apiClient) { api = apiClient; }

    public static String username() { return username; }
    public static void setUsername(String u) { username = u; }

    public static String role() { return role == null ? "STANDARD" : role; }
    public static void setRole(String r) { role = r; }

    public static boolean isAdmin() { return "ADMIN".equalsIgnoreCase(role()); }

    public static String baseUrl() { return baseUrl; }
    public static void setBaseUrl(String url) { baseUrl = url; }
}