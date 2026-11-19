package com.dacs4.filesync;

import com.dacs4.filesync.controller.ShellController;
import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.stage.Stage;

import java.io.IOException;

public class Main extends Application {
    private ShellController shell;
    @Override
    public void start(Stage stage) throws IOException {
        FXMLLoader loader = new FXMLLoader(Main.class.getResource("view/shell.fxml"));
        stage.setScene(new Scene(loader.load(), 1280, 800));
        shell = loader.getController();
        stage.setTitle("DistributedFileSync | DACS4");
        stage.setMinWidth(1050);
        stage.setMinHeight(680);
        stage.setWidth(1280);
        stage.setHeight(800);
        stage.show();
    }

    @Override
    public void stop() {
        if (shell != null) {
            shell.closeConnection();
        }
    }

    public static void main(String[] args) {
        launch(args);
    }
}
