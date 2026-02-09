package uk.ac.ntu.cloudfs.ui;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import javafx.application.Platform;
import javafx.fxml.FXML;
import javafx.scene.control.*;
import javafx.scene.layout.BorderPane;
import javafx.scene.layout.StackPane;
import javafx.scene.layout.VBox;
import javafx.stage.FileChooser;

public final class MainController {

    // Terminal supported commands (UI help + "help" command output)
    private static final String[] SUPPORTED_COMMANDS = {
            "whoami",
            "pwd",
            "ls",
            "tree",
            "mkdir <dir>",
            "cd <dir>",
            "cat <file>",
            "touch <file>",
            "rm <file>",
            "ps",
            "help",
            "clear"
    };

    private final ExecutorService bg = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "ui-bg");
        t.setDaemon(true);
        return t;
    });

    private final AtomicInteger busyCount = new AtomicInteger(0);

    private ApiClient api;

    // Root + chrome
    @FXML private BorderPane root;
    @FXML private VBox sidebar;
    @FXML private StackPane contentStack;
    @FXML private StackPane busyOverlay;

    @FXML private TextField lbUrlField;
    @FXML private Label currentUserLabel;
    @FXML private Label appStatusLabel;

    // Nav
    @FXML private Button navTerminalBtn;
    @FXML private Button navFilesBtn;
    @FXML private Button navShareBtn;
    @FXML private Button navAdminBtn;

    // Views
    @FXML private VBox terminalView;
    @FXML private VBox filesView;
    @FXML private VBox shareView;
    @FXML private VBox adminView;

    // Terminal
    @FXML private TextArea terminalOut;
    @FXML private TextField terminalIn;
    @FXML private Label terminalCommandsLabel;
    @FXML private Button terminalSendBtn;

    // Files
    @FXML private ListView<String> filesList;
    @FXML private TextField fileIdField;
    @FXML private Label filesStatusLabel;
    @FXML private Button filesRefreshBtn;
    @FXML private Button filesUploadBtn;
    @FXML private Button filesDownloadBtn;
    @FXML private Button filesDeleteBtn;

    // Share
    @FXML private TextField shareFileIdField;
    @FXML private TextField shareTargetField;
    @FXML private CheckBox shareRead;
    @FXML private CheckBox shareWrite;
    @FXML private Label shareStatusLabel;
    @FXML private Button shareGrantBtn;
    @FXML private Button shareRevokeBtn;

    // Admin
    @FXML private ListView<String> adminUsersList;
    @FXML private TextField adminUserField;
    @FXML private TextField adminPassField;
    @FXML private ComboBox<String> adminRoleBox;
    @FXML private Label adminStatusLabel;
    @FXML private TextArea adminAuditOut;

    @FXML private Button adminRefreshBtn;
    @FXML private Button adminAuditBtn;
    @FXML private Button adminCreateBtn;
    @FXML private Button adminSetRoleBtn;
    @FXML private Button adminDeleteBtn;

    private String currentUser = "-";
    private String currentRole = "STANDARD";

    @FXML
    private void initialize() {
        // Session-aware boot
        if (Session.api() != null) {
            api = Session.api();
            currentUser = Session.username() == null ? "-" : Session.username();
            currentRole = Session.role();

            lbUrlField.setText(Session.baseUrl() == null ? "" : Session.baseUrl());
            lbUrlField.setDisable(true);

            setUserLabel(currentUser + " (" + currentRole + ")");
        } else {
            String baseUrl = System.getenv().getOrDefault("LB_URL", "http://localhost:8081");
            lbUrlField.setText(baseUrl);
            resetApi(baseUrl);
            setUserLabel("-");
        }

        // Admin visibility
        if (navAdminBtn != null) {
            boolean admin = Session.isAdmin();
            navAdminBtn.setVisible(admin);
            navAdminBtn.setManaged(admin);
        }

        // Roles dropdown
        if (adminRoleBox != null) {
            adminRoleBox.getItems().setAll("STANDARD", "ADMIN");
            if (adminRoleBox.getValue() == null) adminRoleBox.setValue("STANDARD");
        }

        // Terminal UI
        if (terminalCommandsLabel != null) {
            terminalCommandsLabel.setText("Commands: " + String.join(" | ", SUPPORTED_COMMANDS));
        }
        if (terminalIn != null) {
            terminalIn.setPromptText("Type a command (try: help)");
            terminalIn.setOnAction(e -> onTerminalSend()); // Enter sends
        }

        // Default view
        showView("terminal");
        setAppStatus("");
    }

    // ---------------- NAV ----------------
    @FXML private void goTerminal() { showView("terminal"); }
    @FXML private void goFiles()    { showView("files"); }
    @FXML private void goShare()    { showView("share"); }
    @FXML private void goAdmin()    { showView("admin"); }

    private void showView(String which) {
        setViewVisible(terminalView, "terminal".equals(which));
        setViewVisible(filesView,    "files".equals(which));
        setViewVisible(shareView,    "share".equals(which));
        setViewVisible(adminView,    "admin".equals(which));

        // tiny UX touch: clear status bar when switching
        setAppStatus("");
    }

    private static void setViewVisible(VBox view, boolean on) {
        if (view == null) return;
        view.setVisible(on);
        view.setManaged(on);
    }

    // ---------------- LOGOUT ----------------
    @FXML
    private void onLogout() {
        try { Session.clear(); } catch (Exception ignored) {}
        SceneManager.switchTo("/ui/login.fxml", 520, 360);
    }

    // ---------------- TERMINAL ----------------
    @FXML
    public void onTerminalSend() {
        String cmdRaw = terminalIn.getText();
        if (cmdRaw == null || cmdRaw.isBlank()) return;

        final String cmd = cmdRaw.trim();
        terminalIn.clear();

        if ("clear".equalsIgnoreCase(cmd)) {
            if (terminalOut != null) terminalOut.clear();
            setAppStatus("Cleared terminal.");
            return;
        }

        if (terminalOut != null) terminalOut.appendText("> " + cmd + "\n");

        if ("help".equalsIgnoreCase(cmd)) {
            if (terminalOut != null) terminalOut.appendText(String.join("\n", SUPPORTED_COMMANDS) + "\n");
            setAppStatus("Listed available commands.");
            return;
        }

        runBg(() -> api.post("/api/term?cmd=" + ApiClient.enc(cmd)),
                ok -> {
                    if (ok != null && !ok.isBlank() && terminalOut != null) terminalOut.appendText(ok + "\n");
                    setAppStatus("Command executed.");
                },
                err -> {
                    if (terminalOut != null) terminalOut.appendText("Error: " + err + "\n");
                    setAppStatus("Terminal error.");
                });
    }

    // ---------------- FILES ----------------
    @FXML
    public void onFilesRefresh() {
        setStatus(filesStatusLabel, "Working...");
        runBg(() -> api.getText("/api/files"),
                ok -> {
                    List<String> items = ok == null || ok.isBlank() ? List.of() : Arrays.asList(ok.split("\\R"));
                    if (filesList != null) filesList.getItems().setAll(items);
                    setStatus(filesStatusLabel, "Listed " + items.size() + " file(s).");
                    setAppStatus("Files refreshed.");
                },
                err -> {
                    setStatus(filesStatusLabel, "Error: " + err);
                    setAppStatus("Files refresh failed.");
                });
    }

    @FXML
    public void onFilesUpload() {
        String id = fileIdField.getText().trim();
        if (id.isBlank()) { setStatus(filesStatusLabel, "Enter fileId first."); return; }

        FileChooser fc = new FileChooser();
        fc.setTitle("Choose file to upload");
        File chosen = fc.showOpenDialog(root.getScene().getWindow());
        if (chosen == null) return;

        setStatus(filesStatusLabel, "Working...");
        runBg(() -> {
                    api.putFile("/api/file?fileId=" + ApiClient.enc(id) + "&chunkSize=32768", chosen.toPath());
                    return "Uploaded " + id;
                },
                ok -> {
                    setStatus(filesStatusLabel, ok);
                    setAppStatus("Upload complete.");
                    onFilesRefresh();
                },
                err -> {
                    setStatus(filesStatusLabel, "Error: " + err);
                    setAppStatus("Upload failed.");
                });
    }

    @FXML
    public void onFilesDownload() {
        String selected = (filesList == null) ? null : filesList.getSelectionModel().getSelectedItem();
        if (selected == null || selected.isBlank()) { setStatus(filesStatusLabel, "Select a fileId first."); return; }

        FileChooser fc = new FileChooser();
        fc.setTitle("Save downloaded file as…");
        fc.setInitialFileName(selected);
        File out = fc.showSaveDialog(root.getScene().getWindow());
        if (out == null) return;

        setStatus(filesStatusLabel, "Working...");
        runBg(() -> {
                    api.downloadTo("/api/file?fileId=" + ApiClient.enc(selected), out.toPath());
                    return "Downloaded " + selected;
                },
                ok -> {
                    setStatus(filesStatusLabel, ok);
                    setAppStatus("Download complete.");
                },
                err -> {
                    setStatus(filesStatusLabel, "Error: " + err);
                    setAppStatus("Download failed.");
                });
    }

    @FXML
    public void onFilesDelete() {
        String selected = (filesList == null) ? null : filesList.getSelectionModel().getSelectedItem();
        if (selected == null || selected.isBlank()) { setStatus(filesStatusLabel, "Select a fileId first."); return; }

        Alert confirm = new Alert(Alert.AlertType.CONFIRMATION);
        confirm.setTitle("Delete");
        confirm.setHeaderText("Delete fileId: " + selected + " ?");
        confirm.setContentText("This will remove metadata and stored chunks.");
        var res = confirm.showAndWait();
        if (res.isEmpty() || res.get() != ButtonType.OK) return;

        setStatus(filesStatusLabel, "Working...");
        runBg(() -> api.delete("/api/file?fileId=" + ApiClient.enc(selected)),
                ok -> {
                    setStatus(filesStatusLabel, ok);
                    setAppStatus("File deleted.");
                    onFilesRefresh();
                },
                err -> {
                    setStatus(filesStatusLabel, "Error: " + err);
                    setAppStatus("Delete failed.");
                });
    }

    // ---------------- SHARE ----------------
    @FXML
    public void onShareGrant() {
        String f = shareFileIdField.getText().trim();
        String t = shareTargetField.getText().trim();
        if (f.isBlank() || t.isBlank()) { setStatus(shareStatusLabel, "Enter fileId and target user."); return; }

        boolean r = shareRead.isSelected();
        boolean w = shareWrite.isSelected();

        setStatus(shareStatusLabel, "Working...");
        runBg(() -> api.post("/api/acl/grant?fileId=" + ApiClient.enc(f)
                        + "&target=" + ApiClient.enc(t)
                        + "&read=" + r
                        + "&write=" + w),
                ok -> {
                    setStatus(shareStatusLabel, ok);
                    setAppStatus("Permissions granted.");
                },
                err -> {
                    setStatus(shareStatusLabel, "Error: " + err);
                    setAppStatus("Grant failed.");
                });
    }

    @FXML
    public void onShareRevoke() {
        String f = shareFileIdField.getText().trim();
        String t = shareTargetField.getText().trim();
        if (f.isBlank() || t.isBlank()) { setStatus(shareStatusLabel, "Enter fileId and target user."); return; }

        setStatus(shareStatusLabel, "Working...");
        runBg(() -> api.post("/api/acl/revoke?fileId=" + ApiClient.enc(f)
                        + "&target=" + ApiClient.enc(t)),
                ok -> {
                    setStatus(shareStatusLabel, ok);
                    setAppStatus("Permissions revoked.");
                },
                err -> {
                    setStatus(shareStatusLabel, "Error: " + err);
                    setAppStatus("Revoke failed.");
                });
    }

    // ---------------- ADMIN ----------------
    @FXML
    public void onAdminRefresh() {
        setStatus(adminStatusLabel, "Working...");
        runBg(() -> api.getText("/api/admin/users"),
                ok -> {
                    List<String> items = ok == null || ok.isBlank() ? List.of() : Arrays.asList(ok.split("\\R"));
                    if (adminUsersList != null) adminUsersList.getItems().setAll(items);
                    setStatus(adminStatusLabel, "Listed " + items.size() + " user(s).");
                    setAppStatus("Users refreshed.");
                },
                err -> {
                    setStatus(adminStatusLabel, "Error: " + err);
                    setAppStatus("Admin refresh failed.");
                });
    }

    @FXML
    public void onAdminCreate() {
        String u = adminUserField.getText().trim();
        String p = adminPassField.getText();
        String r = adminRoleBox.getSelectionModel().getSelectedItem();
        if (u.isBlank() || p == null || p.isBlank()) { setStatus(adminStatusLabel, "Enter username + password."); return; }

        setStatus(adminStatusLabel, "Working...");
        runBg(() -> api.post("/api/admin/create?username=" + ApiClient.enc(u)
                        + "&password=" + ApiClient.enc(p)
                        + "&role=" + ApiClient.enc(r)),
                ok -> {
                    setStatus(adminStatusLabel, ok);
                    setAppStatus("User created.");
                    onAdminRefresh();
                },
                err -> {
                    setStatus(adminStatusLabel, "Error: " + err);
                    setAppStatus("Create user failed.");
                });
    }

    @FXML
    public void onAdminAudit() {
        if (adminAuditOut != null) adminAuditOut.setText("");
        setStatus(adminStatusLabel, "Working...");

        runBg(() -> api.getText("/api/admin/audit?limit=200"),
                ok -> {
                    if (adminAuditOut != null) adminAuditOut.setText(ok == null ? "" : ok);
                    setStatus(adminStatusLabel, "Audit loaded.");
                    setAppStatus("Audit loaded.");
                },
                err -> {
                    setStatus(adminStatusLabel, "Error: " + err);
                    setAppStatus("Audit failed.");
                });
    }

    @FXML
    public void onAdminSetRole() {
        String u = adminUserField.getText().trim();
        String r = adminRoleBox.getSelectionModel().getSelectedItem();
        if (u.isBlank()) { setStatus(adminStatusLabel, "Enter username."); return; }

        setStatus(adminStatusLabel, "Working...");
        runBg(() -> api.post("/api/admin/role?username=" + ApiClient.enc(u) + "&role=" + ApiClient.enc(r)),
                ok -> {
                    setStatus(adminStatusLabel, ok);
                    setAppStatus("Role updated.");
                    onAdminRefresh();
                },
                err -> {
                    setStatus(adminStatusLabel, "Error: " + err);
                    setAppStatus("Set role failed.");
                });
    }

    @FXML
    public void onAdminDelete() {
        String u = adminUserField.getText().trim();
        if (u.isBlank()) { setStatus(adminStatusLabel, "Enter username."); return; }

        setStatus(adminStatusLabel, "Working...");
        runBg(() -> api.delete("/api/admin/users?username=" + ApiClient.enc(u)),
                ok -> {
                    setStatus(adminStatusLabel, ok);
                    setAppStatus("User deleted.");
                    onAdminRefresh();
                },
                err -> {
                    setStatus(adminStatusLabel, "Error: " + err);
                    setAppStatus("Delete user failed.");
                });
    }

    // ---------------- BUSY + HELPERS ----------------
    private void resetApi(String baseUrl) {
        String tok = (api == null) ? null : api.token();
        api = new ApiClient(baseUrl);
        if (tok != null && !tok.isBlank()) api.setToken(tok);
    }

    private void setStatus(Label label, String msg) {
        Platform.runLater(() -> {
            if (label != null) label.setText(msg == null ? "" : msg);
        });
    }

    private void setUserLabel(String user) {
        Platform.runLater(() -> {
            if (currentUserLabel != null) currentUserLabel.setText(user == null || user.isBlank() ? "-" : user);
        });
    }

    private void setAppStatus(String msg) {
        Platform.runLater(() -> {
            if (appStatusLabel != null) appStatusLabel.setText(msg == null ? "" : msg);
        });
    }

    private void busyOn() {
        int n = busyCount.incrementAndGet();
        if (n == 1) {
            Platform.runLater(() -> {
                if (busyOverlay != null) {
                    busyOverlay.setVisible(true);
                    busyOverlay.setManaged(true);
                }
                if (sidebar != null) sidebar.setDisable(true);
                if (contentStack != null) contentStack.setDisable(true); // overlay is still visible
                // re-enable overlay so it can show
                if (busyOverlay != null) busyOverlay.setDisable(false);
            });
        }
    }

    private void busyOff() {
        int n = busyCount.decrementAndGet();
        if (n <= 0) {
            busyCount.set(0);
            Platform.runLater(() -> {
                if (busyOverlay != null) {
                    busyOverlay.setVisible(false);
                    busyOverlay.setManaged(false);
                }
                if (sidebar != null) sidebar.setDisable(false);
                if (contentStack != null) contentStack.setDisable(false);
            });
        }
    }

    private interface Work<T> { T run() throws Exception; }

    private <T> void runBg(Work<T> work, java.util.function.Consumer<T> ok, java.util.function.Consumer<String> err) {
        busyOn();
        bg.submit(() -> {
            try {
                T res = work.run();
                Platform.runLater(() -> ok.accept(res));
            } catch (Exception e) {
                Platform.runLater(() -> err.accept(e.getMessage()));
            } finally {
                busyOff();
            }
        });
    }
}