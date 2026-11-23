package com.dacs4.filesync.controller;

import com.dacs4.filesync.network.ConnectionState;
import com.dacs4.filesync.network.FileTransferClient;
import com.dacs4.filesync.service.ConnectionService;
import com.dacs4.filesync.service.FileService;
import com.dacs4.filesync.shared.dto.FileMetadata;
import com.dacs4.filesync.shared.exception.ProtocolException;
import com.dacs4.filesync.util.Views;
import javafx.animation.Animation;
import javafx.animation.PauseTransition;
import javafx.beans.binding.Bindings;
import javafx.collections.FXCollections;
import javafx.collections.ListChangeListener;
import javafx.collections.ObservableList;
import javafx.collections.transformation.FilteredList;
import javafx.collections.transformation.SortedList;
import javafx.fxml.FXML;
import javafx.scene.control.*;
import javafx.stage.FileChooser;
import javafx.util.Duration;

import java.net.SocketTimeoutException;
import java.nio.file.FileAlreadyExistsException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

public final class FilesController {
    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("dd/MM/uuuu HH:mm")
        .withZone(ZoneId.systemDefault());

    @FXML private TextField search, selectedPath;
    @FXML private javafx.scene.layout.HBox pathRow;
    @FXML private Label count, transferStatus;
    @FXML private TableView<FileMetadata> files;
    @FXML private Button upload, download, refresh, cancel, clearFilters, openVersionHistory;
    private java.util.function.Consumer<String> openHistory;
    @FXML private ProgressBar transferProgress;

    private final ObservableList<FileMetadata> serverFiles = FXCollections.observableArrayList();
    private final ObservableList<FileMetadata> serverFilesView = FXCollections.unmodifiableObservableList(serverFiles);
    private final javafx.beans.property.ReadOnlyBooleanWrapper listingReady = new javafx.beans.property.ReadOnlyBooleanWrapper();
    private final Label listPlaceholder = new Label();
    private boolean listingFailed;
    private final FilteredList<FileMetadata> filtered = new FilteredList<>(serverFiles);
    private ConnectionService connection;
    private FileService transfers;
    private boolean active;
    private boolean refreshPending;
    private final PauseTransition notificationRefresh = new PauseTransition(Duration.millis(350));

    @FXML private void initialize() {
        Views.table(files, "Kết nối máy chủ để xem danh sách tệp.");
        files.setPlaceholder(listPlaceholder);
        var selection = files.getSelectionModel().selectedItemProperty();
        pathRow.visibleProperty().bind(selection.isNotNull());
        pathRow.managedProperty().bind(pathRow.visibleProperty());
        selectedPath.textProperty().bind(Bindings.createStringBinding(
            () -> selection.get() == null ? "" : selection.get().relativePath(), selection));
        files.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY_FLEX_NEXT_COLUMN);
        var name = Views.column(files, "Tên tệp", 230, FileMetadata::name);
        name.setPrefWidth(460);
        name.setCellFactory(column -> new TableCell<>() {
            @Override protected void updateItem(String path, boolean empty) {
                super.updateItem(path, empty);
                setText(empty || path == null ? null : Views.filenameFirst(path));
                setTooltip(empty || path == null ? null : Views.fullText(path));
            }
        });
        var size = Views.column(files, "Dung lượng", 125, FileMetadata::size);
        size.getStyleClass().add("numeric-column");
        size.setCellFactory(column -> new TableCell<>() {
            @Override
            protected void updateItem(Long bytes, boolean empty) {
                super.updateItem(bytes, empty);
                setText(empty || bytes == null ? null : String.format(Locale.forLanguageTag("vi-VN"), "%,d byte", bytes));
            }
        });
        var modified = Views.column(files, "Sửa đổi trên máy chủ", 180, FileMetadata::modifiedMillis);
        modified.setCellFactory(column -> new TableCell<>() {
            @Override
            protected void updateItem(Long millis, boolean empty) {
                super.updateItem(millis, empty);
                setText(empty || millis == null ? null : DATE.format(Instant.ofEpochMilli(millis)));
            }
        });
        SortedList<FileMetadata> sorted = new SortedList<>(filtered);
        sorted.comparatorProperty().bind(files.comparatorProperty());
        files.setItems(sorted);
        search.textProperty().addListener((observable, before, after) -> filter());
        filtered.addListener((ListChangeListener<FileMetadata>) change -> {
            updateCount();
        });
        filter();
    }

    void bindConnection(ConnectionService service) {
        connection = service;
        transfers = new FileService(service);
        var status = service.statusProperty();
        var unavailable = Bindings.createBooleanBinding(
            () -> status.get().state() != ConnectionState.CONNECTED, status).or(transfers.busyProperty());
        upload.disableProperty().bind(unavailable);
        refresh.disableProperty().bind(unavailable);
        download.disableProperty().bind(unavailable.or(files.getSelectionModel().selectedItemProperty().isNull()));
        openVersionHistory.disableProperty().bind(Bindings.createBooleanBinding(
            () -> status.get().state() != ConnectionState.CONNECTED, status)
            .or(files.getSelectionModel().selectedItemProperty().isNull()));
        cancel.disableProperty().bind(transfers.cancellableProperty().not());
        cancel.visibleProperty().bind(transfers.cancellableProperty());
        cancel.managedProperty().bind(cancel.visibleProperty());
        transferProgress.progressProperty().bind(transfers.progressProperty());
        transferProgress.visibleProperty().bind(transfers.busyProperty());
        transferProgress.managedProperty().bind(transferProgress.visibleProperty());
        notificationRefresh.setOnFinished(event -> {
            refreshIfPending();
        });
        service.setFileChangeListener(notification -> {
            markServerFilesStale();
        });
        service.setLocalUploadCommittedListener(() -> {
            markServerFilesStale();
        });
        status.addListener((observable, before, after) -> {
            notificationRefresh.stop();
            if (after.state() == ConnectionState.CONNECTED) {
                updateCount();
                refresh();
            } else {
                listingReady.set(false);
                listingFailed = false;
                refreshPending = false;
                serverFiles.clear();
                updateCount();
                transferStatus.setText("");
            }
        });
    }

    ObservableList<FileMetadata> serverFiles() {
        return serverFilesView;
    }

    javafx.beans.property.ReadOnlyBooleanProperty listingReadyProperty() {
        return listingReady.getReadOnlyProperty();
    }

    javafx.beans.property.ReadOnlyBooleanProperty busyProperty() {
        return transfers.busyProperty();
    }

    void setOpenHistoryAction(java.util.function.Consumer<String> action) {
        openHistory = action;
    }

    @FXML private void openVersionHistory() {
        var selected = files.getSelectionModel().getSelectedItem();
        if (selected != null && openHistory != null) {
            openHistory.accept(selected.relativePath());
        }
    }

    private void markServerFilesStale() {
        refreshPending = true;
        if (active && notificationRefresh.getStatus() != Animation.Status.RUNNING) {
            notificationRefresh.playFromStart();
        }
    }

    void cancelTransferOnClose() {
        notificationRefresh.stop();
        if (transfers != null) {
            transfers.cancel();
        }
    }

    void setActive(boolean active) {
        if (this.active == active) {
            return;
        }
        this.active = active;
        notificationRefresh.stop();
        refreshIfPending();
    }

    private void filter() {
        String query = search.getText().strip().toLowerCase(Locale.ROOT);
        filtered.setPredicate(file -> file.name().toLowerCase(Locale.ROOT).contains(query));
        clearFilters.setDisable(search.getText().isEmpty());
        updateCount();
    }

    private void updateCount() {
        boolean connected = connection != null && connection.connectedClient() != null;
        count.setText(listingReady.get() ? "Hiển thị " + filtered.size() + "/" + serverFiles.size() + " tệp" : "");
        listPlaceholder.setText(!connected ? "Kết nối máy chủ để xem danh sách tệp."
            : listingFailed ? "Không tải được danh sách. Chọn Làm mới để thử lại."
            : !listingReady.get() ? "Đang tải danh sách tệp…"
            : serverFiles.isEmpty() ? "Máy chủ chưa có tệp."
            : "Không có tệp khớp với tìm kiếm.");
    }

    @FXML private void clearFilters() {
        search.clear();
        search.requestFocus();
    }

    @FXML void refresh() {
        refreshPending = true;
        notificationRefresh.stop();
        refreshIfPending();
    }

    private void refreshIfPending() {
        if (!active || !refreshPending || transfers.busyProperty().get()) {
            return;
        }
        var session = connection.connectedClient();
        if (session == null) {
            return;
        }
        notificationRefresh.stop();
        refreshPending = false;
        listingFailed = false;
        updateCount();
        transfers.list(result -> {
            if (session == connection.connectedClient()) {
                if (!serverFiles.equals(result)) {
                    FileMetadata selected = files.getSelectionModel().getSelectedItem();
                    serverFiles.setAll(result);
                    if (selected != null) {
                        filtered.stream().filter(file -> file.name().equals(selected.name())).findFirst()
                            .ifPresent(files.getSelectionModel()::select);
                    }
                    updateCount();
                }
                listingReady.set(true);
                updateCount();
            }
            refreshAfterOperation();
        }, error -> {
            if (session != connection.connectedClient()) {
                refreshAfterOperation();
                return;
            }
            listingFailed = true;
            updateCount();
            if (listingReady.get()) {
                showTransferStatus("Không cập nhật được danh sách. Chọn Làm mới để thử lại.", "danger-text");
            }
            if (refreshPending) {
                refreshAfterOperation();
            } else {
                // Retry on the next notification or page open, without retrying in a loop.
                refreshPending = true;
            }
        });
    }

    @FXML private void upload() {
        FileChooser chooser = new FileChooser();
        chooser.setTitle("Chọn tệp tải lên máy chủ");
        var source = chooser.showOpenDialog(files.getScene().getWindow());
        if (source == null || upload.isDisable()) {
            return;
        }
        showTransferStatus("Đang tải lên " + source.getName() + "…", "muted-text");
        transfers.upload(source.toPath(), () -> finished("Đã tải lên " + source.getName() + "."), this::fileOperationFailed);
    }

    @FXML private void download() {
        FileMetadata selected = files.getSelectionModel().getSelectedItem();
        if (selected == null || download.isDisable()) {
            return;
        }
        showTransferStatus("Đang tải xuống " + Views.filenameFirst(selected.name()) + "…", "muted-text");
        transfers.download(selected, path -> finished("Đã lưu tại " + path), this::fileOperationFailed);
    }

    @FXML private void cancel() {
        transfers.cancel();
        showTransferStatus("Đang hủy thao tác…", "muted-text");
    }

    private void finished(String message) {
        showTransferStatus(message, "success-text");
        refresh();
    }

    private void failed(Throwable error) {
        showTransferStatus(switch (error) {
            case FileTransferClient.Cancelled ignored -> "Đã hủy truyền tệp.";
            case FileTransferClient.Failure failure -> failure.getMessage();
            case FileAlreadyExistsException ignored -> "Tệp cùng tên đã có trong thư mục tải xuống.";
            case SocketTimeoutException ignored -> "Máy chủ không phản hồi đúng hạn.";
            case ProtocolException ignored -> "Phản hồi truyền tệp không hợp lệ.";
            default -> "Không thể hoàn thành thao tác. Kiểm tra kết nối và quyền đọc hoặc ghi tệp.";
        }, error instanceof FileTransferClient.Cancelled ? "muted-text" : "danger-text");
    }

    private void showTransferStatus(String text, String style) {
        transferStatus.setText(text);
        transferStatus.getStyleClass().setAll("label", style);
    }

    private void fileOperationFailed(Throwable error) {
        failed(error);
        refreshAfterOperation();
    }

    private void refreshAfterOperation() {
        if (notificationRefresh.getStatus() != Animation.Status.RUNNING) {
            refreshIfPending();
        }
    }

}
