package com.dacs4.filesync.controller;

import com.dacs4.filesync.network.ConnectionState;
import com.dacs4.filesync.service.ConnectionService;
import com.dacs4.filesync.sync.ConflictRegistry;
import com.dacs4.filesync.util.Views;
import javafx.application.Platform;
import javafx.beans.binding.Bindings;
import javafx.fxml.FXML;
import javafx.scene.control.*;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicBoolean;

public final class DashboardController {
    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("dd/MM/uuuu HH:mm:ss");
    private static final int MAX_ACTIVITY_ROWS = 50;
    private record Activity(String time, String state, String detail) {}

    @FXML private Label serverStatus, fileCount, historyStatus, lastConnected, conflictCount;
    @FXML private TableView<Activity> activities;
    @FXML private Button openConflicts;
    private final AtomicBoolean refreshPosted = new AtomicBoolean();
    private ConflictRegistry registry;
    private Runnable unsubscribe;

    @FXML private void initialize() {
        Views.table(activities, "Chưa có hoạt động kết nối trong phiên này.");
        var time = Views.column(activities, "Thời gian", 155, Activity::time);
        time.setSortable(false);
        time.setMaxWidth(185);
        var state = Views.column(activities, "Trạng thái", 130, Activity::state);
        state.setSortable(false);
        state.setMaxWidth(155);
        Views.column(activities, "Chi tiết", 220, Activity::detail).setSortable(false);
        conflictCount.setText("—");
    }

    void bindConnection(ConnectionService connection, FilesController files) {
        var status = connection.statusProperty();
        var listedFiles = files.serverFiles();
        var listingReady = files.listingReadyProperty();
        fileCount.textProperty().bind(Bindings.createStringBinding(
            () -> listingReady.get() ? Integer.toString(listedFiles.size()) : "—", listingReady, listedFiles));
        updateConnection(connection, status.get());
        status.addListener((property, before, current) -> {
            updateConnection(connection, current);
            String time = TIME.format(LocalDateTime.now());
            activities.getItems().addFirst(new Activity(time, current.state().label(),
                current.detail().isEmpty() ? "—" : current.detail()));
            if (activities.getItems().size() > MAX_ACTIVITY_ROWS) {
                activities.getItems().removeLast();
            }
            if (current.state() == ConnectionState.CONNECTED) {
                updateBadge(lastConnected, time, "info");
            }
        });
    }

    private void updateConnection(ConnectionService connection, ConnectionService.Status status) {
        updateBadge(serverStatus, status.state().label(), status.state().style());
        var client = connection.connectedClient();
        updateBadge(historyStatus, client == null ? "Chưa có kết nối"
            : client.supportsVersionHistory() ? "Có hỗ trợ" : "Không hỗ trợ",
            client == null ? "muted" : client.supportsVersionHistory() ? "success" : "warning");
    }

    private static void updateBadge(Label badge, String text, String style) {
        badge.setText(text);
        badge.getStyleClass().setAll("label", "badge", style);
    }

    void bindConflicts(ConflictRegistry conflicts) {
        close();
        registry = conflicts;
        unsubscribe = conflicts.subscribe(() -> {
            if (refreshPosted.compareAndSet(false, true)) {
                Platform.runLater(() -> {
                    refreshPosted.set(false);
                    updateConflicts();
                });
            }
        });
        updateConflicts();
    }

    private void updateConflicts() {
        if (registry != null) {
            long count = registry.snapshot().stream()
                .filter(entry -> entry.status() != ConflictRegistry.Status.RESOLVED).count();
            conflictCount.setText(Long.toString(count));
        }
    }

    void setOpenConflictsAction(Runnable action) {
        openConflicts.setOnAction(event -> action.run());
    }

    void close() {
        if (unsubscribe != null) {
            unsubscribe.run();
            unsubscribe = null;
        }
        registry = null;
    }
}
