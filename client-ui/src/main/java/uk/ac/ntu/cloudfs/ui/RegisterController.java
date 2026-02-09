package uk.ac.ntu.cloudfs.ui;

import javafx.application.Platform;
import javafx.fxml.FXML;
import javafx.scene.control.Label;
import javafx.scene.control.PasswordField;
import javafx.scene.control.TextField;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class RegisterController {

    private final ExecutorService bg = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "ui-register-bg");
        t.setDaemon(true);
        return t;
    });

    @FXML private TextField lbUrlField;
    @FXML private TextField usernameField;
    @FXML private PasswordField passwordField;
    @FXML private PasswordField confirmPasswordField;
    @FXML private Label statusLabel;

    @FXML
    private void initialize() {
        String baseUrl = System.getenv().getOrDefault("LB_URL", "http://localhost:8081");
        lbUrlField.setText(baseUrl);
        setStatus("");
    }

    @FXML
    private void onBack() {
        SceneManager.switchTo("/ui/login.fxml", 520, 360);
    }

    @FXML
    private void onRegister() {
        String baseUrl = lbUrlField.getText().trim();
        String u = usernameField.getText().trim();
        String p = passwordField.getText();
        String c = confirmPasswordField.getText();

        if (baseUrl.isBlank()) { setStatus("Enter Server URL."); return; }
        if (u.isBlank()) { setStatus("Enter a username."); return; }
        if (p == null || p.isBlank()) { setStatus("Enter a password."); return; }
        if (!p.equals(c)) { setStatus("Passwords do not match."); return; }

        setStatus("Registering...");

        bg.submit(() -> {
            try {
                ApiClient api = new ApiClient(baseUrl);
                String resp = api.post("/api/auth/register?username=" + ApiClient.enc(u) + "&password=" + ApiClient.enc(p));

                Platform.runLater(() -> {
                    setStatus(resp == null ? "REGISTERED" : resp.trim());
                    // On success, go back to login
                    SceneManager.switchTo("/ui/login.fxml", 520, 360);
                });

            } catch (Exception e) {
                Platform.runLater(() -> setStatus("Register failed: " + friendly(e)));
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
}