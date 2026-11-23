package com.dacs4.filesync;

import com.dacs4.filesync.controller.ShellController;
import com.dacs4.filesync.shared.logging.AppLogging;
import com.dacs4.filesync.sync.SyncCoordinator;
import com.dacs4.filesync.sync.SyncTransferManager;
import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.stage.Stage;

import java.io.IOException;
import java.nio.file.InvalidPathException;

public class Main extends Application {
    private static final System.Logger LOG = System.getLogger(Main.class.getName());
    private ShellController shell;
    private SyncCoordinator sync;
    private SyncTransferManager syncTransfers;

    @Override
    public void start(Stage stage) throws IOException {
        AppLogging.configure();
        FXMLLoader loader = new FXMLLoader(Main.class.getResource("view/shell.fxml"));
        stage.setScene(new Scene(loader.load(), 1280, 800));
        shell = loader.getController();
        stage.setTitle("DistributedFileSync | DACS4");
        stage.setMinWidth(1050);
        stage.setMinHeight(680);
        stage.setWidth(1280);
        stage.setHeight(800);
        stage.show();
        try {
            var root = SyncCoordinator.configuredRoot();
            syncTransfers = new SyncTransferManager(root);
            shell.bindAutomaticSync(syncTransfers);
            sync = SyncCoordinator.forCreateSync(root, syncTransfers);
            sync.start();
        } catch (InvalidPathException exception) {
            LOG.log(System.Logger.Level.ERROR, "Invalid local sync folder configuration", exception);
        }
    }

    @Override
    public void stop() {
        if (sync != null) {
            sync.close();
        }
        if (syncTransfers != null) {
            syncTransfers.close();
        }
        if (shell != null) {
            shell.closeConnection();
        }
    }

    public static void main(String[] args) {
        launch(args);
    }
}
