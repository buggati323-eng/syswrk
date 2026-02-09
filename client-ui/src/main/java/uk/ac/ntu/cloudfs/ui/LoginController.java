package uk.ac.ntu.cloudfs.ui;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javafx.application.Platform;
import javafx.fxml.FXML;
import javafx.scene.control.Label;
import javafx.scene.control.PasswordField;
import javafx.scene.control.TextField;

public final class LoginController {

    private final ExecutorService bg = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "ui-login-bg");
        t.setDaemon(true);
        return t;
    });

    @FXML private TextField lbUrlField;
    @FXML private TextField usernameField;
    @FXML private PasswordField passwordField;
    @FXML private Label statusLabel;

    @FXML
    private void initialize() {
        String baseUrl = System.getenv().getOrDefault("LB_URL", "http://localhost:8081");
        lbUrlField.setText(baseUrl);
        setStatus("");
    }

    @FXML
    private void onGoRegister() {
        SceneManager.switchTo("/ui/register.fxml", 560, 420);
    }

    @FXML
    private void onLogin() {
        final String baseUrl = lbUrlField.getText().trim();
        final String u = usernameField.getText().trim();
        final String p = passwordField.getText();

        if (baseUrl.isBlank()) { setStatus("Enter Server URL."); return; }
        if (u.isBlank() || p == null || p.isBlank()) { setStatus("Enter username + password."); return; }

        setStatus("Logging in...");

        bg.submit(() -> {
            try {
                ApiClient api = new ApiClient(baseUrl);
                String resp = api.post("/api/auth/login?username=" + ApiClient.enc(u) + "&password=" + ApiClient.enc(p));

                final String respText = (resp == null) ? "" : resp.trim();

                // parse into finals
                final String[] parsed = parseTokenAndRole(respText);
                final String tokenFinal = parsed[0];
                final String roleFinal  = parsed[1];

                api.setToken(tokenFinal);

                Session.setBaseUrl(baseUrl);
                Session.setApi(api);
                Session.setUsername(u);
                Session.setRole(roleFinal);

                Platform.runLater(() -> {
                    setStatus("Logged in as " + u + " (" + roleFinal + ").");
                    SceneManager.switchTo("/ui/main.fxml", 1000, 650);
                });

            } catch (Exception e) {
                Platform.runLater(() -> setStatus("Login failed: " + friendly(e)));
            }
        });
    }

    private void setStatus(String s) {
        if (statusLabel != null) statusLabel.setText(s == null ? "" : s);
    }

    private static String friendly(Exception e) {
        String msg = e.getMessage();
        return (msg == null || msg.isBlank()) ? e.getClass().getSimpleName() : msg.trim();
    }

    private static String[] parseTokenAndRole(String t) {
        String token = "";
        String role = "STANDARD";

        if (t != null) t = t.trim();
        if (t == null) t = "";

        if (t.startsWith("TOKEN ")) {
            String[] parts = t.split("\\s+");
            if (parts.length >= 2) token = parts[1];

            for (int i = 0; i + 1 < parts.length; i++) {
                if ("ROLE".equalsIgnoreCase(parts[i])) {
                    role = parts[i + 1];
                    break;
                }
            }
        } else {
            token = t;
        }

        return new String[]{ token, role };
    }
}