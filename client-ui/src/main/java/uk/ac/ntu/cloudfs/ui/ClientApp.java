package uk.ac.ntu.cloudfs.ui;

import javafx.application.Application;
import javafx.stage.Stage;

public final class ClientApp extends Application {

    @Override
public void start(Stage stage) {
    SceneManager.init(stage);

    stage.setTitle("CloudFS Client");

    
    SceneManager.switchTo("/ui/login.fxml", 520, 360);
}

    public static void main(String[] args) {
        launch(args);
    }
}