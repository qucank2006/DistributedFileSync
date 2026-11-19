package com.dacs4.filesync.controller;

import com.dacs4.filesync.network.ConnectionState;
import com.dacs4.filesync.network.FileTransferClient;
import com.dacs4.filesync.service.ConnectionService;
import com.dacs4.filesync.service.FileService;
import com.dacs4.filesync.shared.dto.FileMetadata;
import com.dacs4.filesync.shared.exception.ProtocolException;
import com.dacs4.filesync.util.Views;
import javafx.beans.binding.Bindings;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.collections.transformation.FilteredList;
import javafx.collections.transformation.SortedList;
import javafx.fxml.FXML;
import javafx.scene.control.*;
import javafx.stage.FileChooser;

import java.net.SocketTimeoutException;
import java.nio.file.FileAlreadyExistsException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

public final class FilesController {
    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("dd/MM/uuuu HH:mm")
        .withZone(ZoneId.systemDefault());

    @FXML private TextField search;
    @FXML private Label count, transferStatus;
    @FXML private TableView<FileMetadata> files;
    @FXML private Button upload, download, refresh, cancel, clearFilters;
    @FXML private ProgressBar transferProgress;

    private final ObservableList<FileMetadata> serverFiles = FXCollections.observableArrayList();
    private final FilteredList<FileMetadata> filtered = new FilteredList<>(serverFiles);
    private ConnectionService connection;
    private FileService transfers;
    private boolean refreshPending;

    @FXML private void initialize() {
        Views.table(files, "Kết nối máy chủ để xem danh sách tệp.");
        files.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY_FLEX_NEXT_COLUMN);
        Views.column(files, "Tên tệp", 230, FileMetadata::name).setPrefWidth(360);
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
        filtered.addListener((javafx.collections.ListChangeListener<FileMetadata>) change -> updateCount());
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
        cancel.disableProperty().bind(transfers.cancellableProperty().not());
        transferProgress.progressProperty().bind(transfers.progressProperty());
        transferProgress.visibleProperty().bind(transfers.busyProperty());
        transferProgress.managedProperty().bind(transferProgress.visibleProperty());
        status.addListener((observable, before, after) -> {
            if (after.state() == ConnectionState.CONNECTED) {
                refresh();
            } else {
                refreshPending = false;
                serverFiles.clear();
                updateCount();
                files.setPlaceholder(new Label("Kết nối máy chủ để xem danh sách tệp."));
            }
        });
    }

    void cancelTransferOnClose() {
        if (transfers != null) {
            transfers.cancel();
        }
    }

    private void filter() {
        String query = search.getText().strip().toLowerCase(Locale.ROOT);
        filtered.setPredicate(file -> file.name().toLowerCase(Locale.ROOT).contains(query));
        clearFilters.setDisable(search.getText().isEmpty());
        updateCount();
    }

    private void updateCount() {
        count.setText("Hiển thị " + filtered.size() + "/" + serverFiles.size() + " tệp");
    }

    @FXML private void clearFilters() {
        search.clear();
        search.requestFocus();
    }

    @FXML private void refresh() {
        var session = connection.connectedClient();
        if (session == null) {
            return;
        }
        if (transfers.busyProperty().get()) {
            refreshPending = true;
            return;
        }
        refreshPending = false;
        transfers.list(result -> {
            if (session == connection.connectedClient()) {
                serverFiles.setAll(result);
                updateCount();
                files.setPlaceholder(new Label("Không có tệp máy chủ phù hợp."));
            }
            refreshIfPending();
        }, this::failed);
    }

    @FXML private void upload() {
        FileChooser chooser = new FileChooser();
        chooser.setTitle("Chọn tệp tải lên máy chủ");
        var source = chooser.showOpenDialog(files.getScene().getWindow());
        if (source == null || upload.isDisable()) {
            return;
        }
        transferStatus.setText("Đang tải lên " + source.getName() + "…");
        transfers.upload(source.toPath(), () -> finished("Đã tải lên " + source.getName() + "."), this::failed);
    }

    @FXML private void download() {
        FileMetadata selected = files.getSelectionModel().getSelectedItem();
        if (selected == null || download.isDisable()) {
            return;
        }
        transferStatus.setText("Đang tải xuống " + selected.name() + "…");
        transfers.download(selected, path -> finished("Đã lưu tại " + path), this::failed);
    }

    @FXML private void cancel() {
        transfers.cancel();
        transferStatus.setText("Đang hủy thao tác…");
    }

    private void finished(String message) {
        transferStatus.setText(message);
        refresh();
    }

    private void failed(Throwable error) {
        transferStatus.setText(switch (error) {
            case FileTransferClient.Cancelled ignored -> "Đã hủy truyền tệp.";
            case FileTransferClient.Failure failure -> failure.getMessage();
            case FileAlreadyExistsException ignored -> "Tệp cùng tên đã có trong thư mục tải xuống.";
            case SocketTimeoutException ignored -> "Máy chủ không phản hồi đúng hạn.";
            case ProtocolException ignored -> "Phản hồi truyền tệp không hợp lệ.";
            default -> "Không thể hoàn thành thao tác. Kiểm tra kết nối và quyền đọc hoặc ghi tệp.";
        });
        refreshIfPending();
    }

    private void refreshIfPending() {
        if (refreshPending) {
            refresh();
        }
    }
}
