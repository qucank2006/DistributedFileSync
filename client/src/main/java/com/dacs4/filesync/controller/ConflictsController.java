package com.dacs4.filesync.controller;

import com.dacs4.filesync.sync.ConflictRegistry;
import com.dacs4.filesync.sync.ConflictRegistry.Entry;
import com.dacs4.filesync.service.ConnectionService;
import com.dacs4.filesync.service.FileService;
import com.dacs4.filesync.util.Views;
import javafx.application.Platform;
import javafx.fxml.FXML;
import javafx.scene.control.ComboBox;
import javafx.scene.control.Label;
import javafx.scene.control.ScrollPane;
import javafx.scene.control.TextField;
import javafx.scene.control.TextArea;
import javafx.scene.control.Button;

import java.nio.file.Path;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Objects;

public final class ConflictsController {
    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("dd/MM/uuuu HH:mm:ss")
        .withZone(ZoneId.systemDefault());

    @FXML private ComboBox<Entry> conflict;
    @FXML private Label type, detectedAt, status, explanation, emptyState;
    @FXML private TextField path;
    @FXML private TextArea localHash, expectedHash, serverHash;
    @FXML private ScrollPane details;
    @FXML private Button compare;
    @FXML private Label compareAvailability;

    private final AtomicBoolean refreshPosted = new AtomicBoolean();
    private ConflictRegistry registry;
    private Runnable unsubscribe;
    private Path localRoot;
    private ConnectionService connection;
    private FileService transfers;
    private ConflictDiffDialog comparison;
    private boolean updatingEntries;

    @FXML private void initialize() {
        Views.readableChoices(conflict,
            entry -> statusLabel(entry.status()) + " · " + Views.filenameFirst(entry.path()), Entry::path);
        details.managedProperty().bind(details.visibleProperty());
        emptyState.visibleProperty().bind(details.visibleProperty().not());
        emptyState.managedProperty().bind(emptyState.visibleProperty());
        conflict.valueProperty().addListener((observable, previous, current) -> {
            if (!updatingEntries && !sameEntry(previous, current)) {
                closeComparison();
            }
            display(current);
        });
        compareAvailability.visibleProperty().bind(compareAvailability.textProperty().isNotEmpty());
        compareAvailability.managedProperty().bind(compareAvailability.visibleProperty());
        display(null);
    }

    void bindConnection(ConnectionService service) {
        connection = service;
        transfers = new FileService(service);
        transfers.busyProperty().addListener((property, previous, current) -> updateCompare());
        service.statusProperty().addListener((property, previous, current) -> {
            if (comparison != null && service.connectedClient() == null) {
                comparison.showFailure("Mất kết nối máy chủ. Hãy kết nối và mở lại bản so sánh.");
            }
            updateCompare();
        });
        updateCompare();
    }

    void bind(ConflictRegistry conflicts, Path root) {
        close();
        localRoot = root;
        registry = conflicts;
        unsubscribe = conflicts.subscribe(this::scheduleRefresh);
        refresh();
    }

    void close() {
        closeComparison();
        if (unsubscribe != null) {
            unsubscribe.run();
            unsubscribe = null;
        }
        registry = null;
        localRoot = null;
        updateCompare();
    }

    private void scheduleRefresh() {
        if (refreshPosted.compareAndSet(false, true)) {
            Platform.runLater(() -> {
                refreshPosted.set(false);
                refresh();
            });
        }
    }

    private void refresh() {
        if (registry == null) {
            return;
        }
        Entry selected = conflict.getValue();
        var entries = registry.snapshot();
        if (conflict.getItems().equals(entries)) {
            return;
        }
        Entry next = entries.stream().filter(entry -> sameEntry(selected, entry))
            .findFirst().orElse(entries.isEmpty() ? null : entries.getFirst());
        updatingEntries = true;
        try {
            conflict.getItems().setAll(entries);
            conflict.setValue(next);
        } finally {
            updatingEntries = false;
        }
        if (!sameEntry(selected, next)) {
            closeComparison();
        }
        display(next);
    }

    private void display(Entry entry) {
        conflict.setDisable(conflict.getItems().isEmpty());
        details.setVisible(entry != null);
        updateCompare();
        if (entry == null) {
            return;
        }
        path.setText(entry.path());
        path.setTooltip(Views.fullText(entry.path()));
        type.setText(typeLabel(entry.type()));
        detectedAt.setText(TIME.format(entry.detectedAt()));
        status.setText(statusLabel(entry.status()));
        status.getStyleClass().setAll("label", "badge", switch (entry.status()) {
            case OPEN -> "danger";
            case RESOLVED -> "success";
            case REQUIRES_RECONCILIATION -> "warning";
        });
        localHash.setText(entry.localSha256());
        expectedHash.setText(entry.expectedSha256());
        serverHash.setText(entry.serverSha256());
        explanation.setText(entry.explanation());
    }

    private static boolean sameEntry(Entry a, Entry b) {
        return Objects.equals(a == null ? null : a.id(), b == null ? null : b.id());
    }

    private void updateCompare() {
        var session = connection == null ? null : connection.connectedClient();
        boolean selected = conflict.getValue() != null && localRoot != null;
        boolean connected = session != null && session.isOpen();
        boolean supported = connected && session.supportsReconciliation();
        compare.setDisable(!selected || !supported || comparison != null || transfers.busyProperty().get());
        compareAvailability.setText(!selected || supported ? ""
            : !connected ? "Kết nối máy chủ để so sánh nội dung."
            : "Máy chủ không hỗ trợ kiểm tra bản chụp tệp.");
    }

    @FXML private void compare() {
        if (compare.isDisabled()) {
            return;
        }
        Entry selected = conflict.getValue();
        var session = connection.connectedClient();
        var dialog = new ConflictDiffDialog(compare.getScene().getWindow(),
            compare.getScene().getRoot().getStylesheets(), selected.path());
        comparison = dialog;
        dialog.setOnHidden(event -> {
            dialog.cancelCapture();
            if (comparison == dialog) {
                comparison = null;
                updateCompare();
            }
        });
        dialog.show();
        updateCompare();
        transfers.compare(localRoot, selected.path(), dialog::cancelled, result -> {
            if (comparison == dialog && !dialog.cancelled() && session == connection.connectedClient()
                    && sameEntry(selected, conflict.getValue())) {
                dialog.showResult(result);
            }
        }, error -> {
            if (comparison == dialog && !dialog.cancelled()) {
                dialog.showFailure(error.getMessage() == null ? "Không thể chuẩn bị bản so sánh." : error.getMessage());
            }
        });
    }

    private void closeComparison() {
        if (comparison != null) {
            comparison.cancelCapture();
            comparison.close();
        }
    }

    private static String typeLabel(ConflictRegistry.Type type) {
        return switch (type) {
            case SERVER_BASE_MISMATCH -> "Bản gốc trên máy chủ không khớp";
            case LOCAL_EDITS -> "Có thay đổi cục bộ";
            case LOCAL_CHANGED_DURING_DOWNLOAD -> "Tệp thay đổi trong khi tải";
            case NOTIFICATION_CHAIN -> "Chuỗi cập nhật không đầy đủ";
            case RECONCILIATION -> "Đối chiếu tệp";
        };
    }

    private static String statusLabel(ConflictRegistry.Status status) {
        return switch (status) {
            case OPEN -> "Chưa xử lý";
            case RESOLVED -> "Đã xử lý";
            case REQUIRES_RECONCILIATION -> "Cần đối chiếu";
        };
    }
}
