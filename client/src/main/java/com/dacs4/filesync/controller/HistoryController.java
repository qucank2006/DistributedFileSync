package com.dacs4.filesync.controller;

import com.dacs4.filesync.network.TcpClient;
import com.dacs4.filesync.service.ConnectionService;
import com.dacs4.filesync.service.FileService;
import com.dacs4.filesync.network.FileTransferClient;
import com.dacs4.filesync.shared.dto.FileMetadata;
import com.dacs4.filesync.shared.dto.FileVersion;
import com.dacs4.filesync.shared.protocol.FileTransferCodec;
import com.dacs4.filesync.util.Views;
import javafx.animation.PauseTransition;
import javafx.animation.Animation;
import javafx.beans.binding.Bindings;
import javafx.beans.value.ObservableBooleanValue;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.scene.control.*;
import javafx.scene.input.Clipboard;
import javafx.scene.input.ClipboardContent;
import javafx.util.Duration;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

public final class HistoryController {
    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("dd/MM/uuuu HH:mm:ss.SSS")
        .withZone(ZoneId.systemDefault());
    @FXML private ComboBox<String> file;
    @FXML private TableView<FileVersion> versions;
    @FXML private Button historyRefresh, copyChecksum, restore;
    @FXML private Label historyStatus, selectedDetails, copyStatus, restoreStatus;
    @FXML private TextField checksum, selectedPath;
    @FXML private javafx.scene.layout.HBox selectedPathRow, versionActions, checksumRow;

    private final PauseTransition changeRefresh = new PauseTransition(Duration.millis(350));
    private ConnectionService connection;
    private FileService transfers;
    private boolean active;
    private boolean pending;
    private boolean updatingChoices;
    private boolean restoring;
    private Runnable refreshFiles;
    private ObservableBooleanValue filesBusy;
    private long generation;

    @FXML private void initialize() {
        var placeholder = new Label();
        placeholder.textProperty().bind(historyStatus.textProperty());
        versions.setPlaceholder(placeholder);
        historyStatus.visibleProperty().bind(Bindings.isNotEmpty(versions.getItems()));
        historyStatus.managedProperty().bind(historyStatus.visibleProperty());
        versionActions.visibleProperty().bind(historyStatus.visibleProperty());
        versionActions.managedProperty().bind(versionActions.visibleProperty());
        checksumRow.visibleProperty().bind(historyStatus.visibleProperty());
        checksumRow.managedProperty().bind(checksumRow.visibleProperty());
        versions.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY_FLEX_NEXT_COLUMN);
        Views.column(versions, "Phiên bản", 92, version -> version.id().toString().substring(0, 8)).setSortable(false);
        Views.column(versions, "Lưu trữ", 150, version -> DATE.format(version.archivedAt())).setSortable(false);
        Views.column(versions, "Sửa đổi gốc", 150, version -> DATE.format(Instant.ofEpochMilli(version.modifiedMillis())))
            .setSortable(false);
        var size = Views.column(versions, "Dung lượng", 95, version -> "%,d byte".formatted(version.size()));
        size.setSortable(false);
        size.getStyleClass().add("numeric-column");

        var selection = versions.getSelectionModel().selectedItemProperty();
        selectedPathRow.managedProperty().bind(selectedPathRow.visibleProperty());
        selectedPathRow.visibleProperty().bind(file.valueProperty().isNotNull());
        selectedPath.textProperty().bind(file.valueProperty());
        Views.readableChoices(file, Views::filenameFirst, path -> path);
        copyStatus.managedProperty().bind(copyStatus.visibleProperty());
        copyStatus.visibleProperty().bind(copyStatus.textProperty().isNotEmpty());
        copyChecksum.disableProperty().bind(selection.isNull());
        checksum.disableProperty().bind(selection.isNull());
        selection.addListener((property, before, selected) -> {
            selectedDetails.setText(selected == null ? "Chọn một phiên bản để xem chi tiết."
                : "Phiên bản " + selected.id());
            checksum.setText(selected == null ? "" : selected.sha256());
            checksum.positionCaret(0);
            copyStatus.setText("");
            updateRestoreButton();
        });
        file.valueProperty().addListener((property, before, selected) -> {
            if (!updatingChoices) {
                if (!restoring) {
                    restoreStatus.setText("");
                }
                refresh();
            }
        });
        changeRefresh.setOnFinished(event -> refresh());
        restoreStatus.visibleProperty().bind(restoreStatus.textProperty().isNotEmpty());
        restoreStatus.managedProperty().bind(restoreStatus.visibleProperty());
    }

    void bindConnection(ConnectionService service, ObservableList<FileMetadata> files,
                        ObservableBooleanValue filesBusy, Runnable refreshFiles) {
        connection = service;
        this.refreshFiles = refreshFiles;
        this.filesBusy = filesBusy;
        transfers = new FileService(service);
        transfers.busyProperty().addListener((property, before, after) -> updateRestoreButton());
        filesBusy.addListener((property, before, after) -> updateRestoreButton());
        files.addListener((javafx.collections.ListChangeListener<FileMetadata>) change -> updateChoices(files));
        updateChoices(files);
        service.statusProperty().addListener((property, before, after) -> refresh());
        refresh();
    }

    private void updateChoices(List<FileMetadata> files) {
        String selected = file.getValue();
        var paths = files.stream().map(FileMetadata::relativePath).toList();
        if (!paths.equals(file.getItems())) {
            updatingChoices = true;
            try {
                file.getItems().setAll(paths);
                file.setValue(selected);
            } finally {
                updatingChoices = false;
            }
        }
    }

    void selectFile(String path) {
        file.setValue(FileTransferCodec.validRelativePath(path) ? path : null);
    }

    void focusFileSelector() {
        file.requestFocus();
    }

    void setActive(boolean active) {
        if (this.active != active) {
            this.active = active;
            refresh();
        }
    }

    void confirmedChange(String path) {
        if (!restoring && active && file.getValue() != null && (path == null || path.equals(file.getValue()))
                && changeRefresh.getStatus() != Animation.Status.RUNNING) {
            changeRefresh.playFromStart();
        }
    }

    @FXML private void refresh() {
        changeRefresh.stop();
        generation++;
        pending = false;
        versions.getSelectionModel().clearSelection();
        versions.getItems().clear();
        TcpClient session = connection == null ? null : connection.connectedClient();
        boolean connected = session != null && session.isOpen();
        boolean supported = connected && session.supportsVersionHistory();
        boolean selected = file.getValue() != null;
        file.setDisable(!connected || restoring);
        historyRefresh.setDisable(!supported || !selected || restoring);
        updateRestoreButton();
        if (!connected) {
            showState("Kết nối máy chủ để xem lịch sử phiên bản.");
        } else if (!supported) {
            showState("Máy chủ không hỗ trợ lịch sử phiên bản.");
        } else if (!selected) {
            showState("Chọn một tệp để xem lịch sử phiên bản.");
        } else if (active) {
            showState("Đang tải lịch sử phiên bản…");
            pending = true;
            loadPending();
        }
    }

    private void loadPending() {
        if (!pending || restoring || transfers.busyProperty().get()) {
            return;
        }
        pending = false;
        long request = generation;
        TcpClient session = connection.connectedClient();
        transfers.listVersions(file.getValue(),
            result -> complete(request, session, result, null),
            error -> complete(request, session, List.of(), error));
    }

    private void complete(long request, TcpClient session, List<FileVersion> result, Throwable error) {
        if (request == generation && active && session == connection.connectedClient()) {
            if (!session.isOpen()) {
                refresh();
            } else if (error != null) {
                showState("Không tải được lịch sử phiên bản. Chọn Làm mới để thử lại.");
            } else {
                versions.getItems().setAll(result.stream().sorted(FileVersion.NEWEST_FIRST).toList());
                showState(result.isEmpty() ? "Tệp này chưa có phiên bản đã lưu."
                    : "Đã tải " + result.size() + " phiên bản.");
            }
        }
        loadPending();
    }

    private void showState(String message) {
        historyStatus.setText(message);
    }

    private void updateRestoreButton() {
        TcpClient session = connection == null ? null : connection.connectedClient();
        restore.setDisable(restoring || session == null || !session.isOpen() || !session.supportsRestore()
            || transfers.busyProperty().get() || filesBusy.get() || versions.getSelectionModel().getSelectedItem() == null);
    }

    @FXML private void restore() {
        if (restore.isDisabled()) {
            return;
        }
        FileVersion selected = versions.getSelectionModel().getSelectedItem();
        TcpClient session = connection.connectedClient();
        setRestoring(true);
        restoreStatus.setText("Đang kiểm tra phiên bản hiện tại…");
        restoreStatus.getStyleClass().setAll("label", "muted-text");
        transfers.readRestoreBase(selected.relativePath(), expected -> {
            if (!isRestoreSelectionCurrent(session, selected) || !confirmRestore(session, selected)
                    || !isRestoreSelectionCurrent(session, selected)) {
                finishRestore("", "muted-text");
                return;
            }
            restoreStatus.setText("Đang khôi phục phiên bản trên máy chủ…");
            transfers.restoreVersion(selected.relativePath(), selected.id(), expected, () -> {
                finishRestore("Đã khôi phục phiên bản. Nội dung trước đó đã được lưu vào lịch sử.", "success-text");
                refreshFiles.run();
            }, error -> {
                if (error instanceof FileTransferClient.Failure failure
                        && failure.resultCode() == FileTransferCodec.RESULT_CONFLICT) {
                    finishRestore("Tệp máy chủ đã thay đổi. Hãy kiểm tra lịch sử trước khi thử lại.", "warning-text");
                } else if (session != connection.connectedClient() || !session.isOpen()) {
                    finishRestore("Mất kết nối. Hãy kiểm tra lịch sử để xác nhận kết quả khôi phục.", "warning-text");
                } else {
                    finishRestore("Khôi phục thất bại. " + error.getMessage(), "danger-text");
                }
            });
        }, error -> finishRestore("Không kiểm tra được phiên bản hiện tại. " + error.getMessage(), "danger-text"));
    }

    private boolean isRestoreSelectionCurrent(TcpClient session, FileVersion selected) {
        return active && session == connection.connectedClient() && session.isOpen()
            && selected.equals(versions.getSelectionModel().getSelectedItem());
    }

    private boolean confirmRestore(TcpClient session, FileVersion selected) {
        var confirm = new ButtonType("Khôi phục", ButtonBar.ButtonData.OK_DONE);
        var cancel = new ButtonType("Hủy", ButtonBar.ButtonData.CANCEL_CLOSE);
        var dialog = new Alert(Alert.AlertType.CONFIRMATION,
            selected.relativePath() + "\nPhiên bản " + selected.id()
                + "\nĐã lưu " + DATE.format(selected.archivedAt())
                + "\nNội dung hiện tại trên máy chủ sẽ được lưu vào lịch sử.", confirm, cancel);
        dialog.initOwner(restore.getScene().getWindow());
        dialog.setTitle("Khôi phục phiên bản");
        dialog.setHeaderText("Dùng phiên bản đã chọn làm phiên bản hiện tại?");
        dialog.setGraphic(null);
        dialog.getDialogPane().setPrefWidth(500);
        dialog.getDialogPane().getStylesheets().addAll(restore.getScene().getRoot().getStylesheets());
        var confirmButton = (Button) dialog.getDialogPane().lookupButton(confirm);
        confirmButton.setDefaultButton(false);
        confirmButton.getStyleClass().add("primary");
        confirmButton.disableProperty().bind(Bindings.createBooleanBinding(
            () -> session != connection.connectedClient() || !session.isOpen(), connection.statusProperty()));
        ((Button) dialog.getDialogPane().lookupButton(cancel)).setDefaultButton(true);
        try {
            return dialog.showAndWait().filter(confirm::equals).isPresent();
        } finally {
            confirmButton.disableProperty().unbind();
        }
    }

    private void setRestoring(boolean value) {
        restoring = value;
        if (value) {
            changeRefresh.stop();
        }
        file.setDisable(value);
        historyRefresh.setDisable(value);
        versions.setDisable(value);
        updateRestoreButton();
    }

    private void finishRestore(String message, String style) {
        setRestoring(false);
        restoreStatus.setText(message);
        restoreStatus.getStyleClass().setAll("label", style);
        refresh();
    }

    @FXML private void copyChecksum() {
        ClipboardContent content = new ClipboardContent();
        content.putString(checksum.getText());
        boolean copied = Clipboard.getSystemClipboard().setContent(content);
        copyStatus.setText(copied ? "Đã sao chép." : "Chưa sao chép được.");
    }
}
