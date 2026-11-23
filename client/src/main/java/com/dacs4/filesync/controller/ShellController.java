package com.dacs4.filesync.controller;

import com.dacs4.filesync.Main;
import com.dacs4.filesync.network.ConnectionState;
import com.dacs4.filesync.service.ConnectionService;
import com.dacs4.filesync.sync.SyncTransferManager;
import com.dacs4.filesync.util.Views;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.layout.StackPane;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class ShellController {
    @FXML private Label pageTitle, connectionBadge;
    @FXML private StackPane content;
    @FXML private Button dashboard, files, history, conflicts, devices;

    private final Map<String, Parent> pages = new HashMap<>();
    private final ConnectionService connection = new ConnectionService();
    private FilesController filesController;
    private DashboardController dashboardController;
    private HistoryController historyController;
    private ConflictsController conflictsController;

    @FXML private void initialize() throws IOException {
        Views.icon(dashboard, "M1 1h6v6H1z M10 1h6v6h-6z M1 10h6v6H1z M10 10h6v6h-6z");
        Views.icon(files, "M1 4h6l2 2h7v9H1z");
        Views.icon(history, "M8.5 1a7.5 7.5 0 1 1 0 15a7.5 7.5 0 1 1 0-15 M8.5 4v5l3 2");
        Views.icon(conflicts, "M8.5 1L16 15H1z M8.5 6v4 M8.5 12v1");
        Views.icon(devices, "M1 2h15v10H1z M8.5 12v4 M5 16h7");
        FXMLLoader dashboardLoader = new FXMLLoader(Main.class.getResource("view/dashboard.fxml"));
        pages.put("dashboard", dashboardLoader.load());
        dashboardController = dashboardLoader.getController();
        dashboardController.setOpenConflictsAction(() -> {
            showConflicts();
            conflicts.requestFocus();
        });
        FXMLLoader filesLoader = new FXMLLoader(Main.class.getResource("view/files.fxml"));
        pages.put("files", filesLoader.load());
        filesController = filesLoader.getController();
        filesController.bindConnection(connection);
        dashboardController.bindConnection(connection, filesController);

        FXMLLoader historyLoader = new FXMLLoader(Main.class.getResource("view/history.fxml"));
        pages.put("history", historyLoader.load());
        historyController = historyLoader.getController();
        historyController.bindConnection(connection, filesController.serverFiles(), filesController.busyProperty(),
            filesController::refresh);
        connection.setHistoryChangeListener(historyController::confirmedChange);
        filesController.setOpenHistoryAction(path -> {
            historyController.selectFile(path);
            showHistory();
            historyController.focusFileSelector();
        });
        FXMLLoader conflictsLoader = new FXMLLoader(Main.class.getResource("view/conflicts.fxml"));
        pages.put("conflicts", conflictsLoader.load());
        conflictsController = conflictsLoader.getController();
        conflictsController.bindConnection(connection);
        FXMLLoader devicesLoader = new FXMLLoader(Main.class.getResource("view/devices.fxml"));
        pages.put("devices", devicesLoader.load());
        DevicesController devicesController = devicesLoader.getController();
        devicesController.bindConnection(connection);

        var status = connection.statusProperty();
        updateConnectionBadge(status.get().state());
        status.addListener((observable, before, current) -> updateConnectionBadge(current.state()));
        showDashboard();
    }

    private void updateConnectionBadge(ConnectionState state) {
        connectionBadge.setText(state.label());
        connectionBadge.getStyleClass().setAll("label", "badge", state.style());
    }

    public void bindAutomaticSync(SyncTransferManager manager) {
        conflictsController.bind(manager.conflicts(), manager.localRoot());
        dashboardController.bindConflicts(manager.conflicts());
        connection.setSyncTransfers(manager);
    }

    public void closeConnection() {
        conflictsController.close();
        dashboardController.close();
        historyController.setActive(false);
        if (filesController != null) {
            filesController.cancelTransferOnClose();
        }
        connection.close();
    }

    @FXML private void showDashboard() { show("dashboard", dashboard); }
    @FXML private void showFiles() { show("files", files); }
    @FXML private void showHistory() { show("history", history); }
    @FXML private void showConflicts() { show("conflicts", conflicts); }
    @FXML private void showDevices() { show("devices", devices); }

    private void show(String page, Button button) {
        content.getChildren().setAll(pages.get(page));
        filesController.setActive("files".equals(page) || "history".equals(page) || "dashboard".equals(page));
        historyController.setActive("history".equals(page));
        pageTitle.setText(button.getText());
        for (Button nav : List.of(dashboard, files, history, conflicts, devices)) {
            nav.getStyleClass().remove("active");
        }
        button.getStyleClass().add("active");
        button.requestFocus();
    }
}
