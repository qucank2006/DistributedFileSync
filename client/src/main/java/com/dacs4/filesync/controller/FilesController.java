package com.dacs4.filesync.controller;

import com.dacs4.filesync.model.DemoRecords.*;
import com.dacs4.filesync.service.DemoData;
import com.dacs4.filesync.util.Views;
import javafx.fxml.FXML;
import javafx.scene.control.*;
import javafx.collections.transformation.FilteredList;
import javafx.collections.transformation.SortedList;
import java.util.Locale;

public final class FilesController {
    @FXML private TextField search;
    @FXML private ComboBox<String> status;
    @FXML private Label count;
    @FXML private TableView<FileItem> files;
    @FXML private Button download, sync, clearFilters;

    private FilteredList<FileItem> filtered;

    @FXML private void initialize() {
        Views.table(files, "Không tìm thấy tệp phù hợp.");
        files.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY_FLEX_NEXT_COLUMN);
        Views.column(files, "Tên tệp", 220, FileItem::name).setPrefWidth(300);
        Views.column(files, "Loại", 60, FileItem::type).setPrefWidth(68);
        var size = Views.column(files, "Dung lượng", 100, FileItem::size);
        size.setSortable(false);
        size.getStyleClass().add("numeric-column");
        Views.column(files, "Sửa đổi gần nhất", 150, FileItem::modified).setSortable(false);
        var syncStatus = Views.column(files, "Trạng thái", 122, FileItem::status);
        syncStatus.setPrefWidth(140);
        Views.badges(syncStatus, SyncStatus::label, SyncStatus::style);

        filtered = new FilteredList<>(new DemoData().files());
        SortedList<FileItem> sorted = new SortedList<>(filtered);
        sorted.comparatorProperty().bind(files.comparatorProperty());
        files.setItems(sorted);
        status.getItems().add("Tất cả trạng thái");
        for (SyncStatus value : SyncStatus.values()) {
            status.getItems().add(value.label());
        }
        status.getSelectionModel().selectFirst();
        search.textProperty().addListener((property, oldValue, newValue) -> filter());
        status.valueProperty().addListener((property, oldValue, newValue) -> filter());
        download.disableProperty().bind(files.getSelectionModel().selectedItemProperty().isNull());
        sync.disableProperty().bind(download.disableProperty());
        filter();
    }

    private void filter() {
        String query = search.getText().strip().toLowerCase(Locale.ROOT);
        String selected = status.getValue();
        boolean showAll = selected == null || selected.equals("Tất cả trạng thái");
        filtered.setPredicate(file -> (showAll || file.status().label().equals(selected))
            && (file.name().toLowerCase(Locale.ROOT).contains(query)
                || file.type().toLowerCase(Locale.ROOT).contains(query)));
        count.setText("Hiển thị " + filtered.size() + "/" + filtered.getSource().size() + " tệp");
        clearFilters.setDisable(search.getText().isEmpty() && showAll);
    }

    @FXML private void clearFilters() {
        search.requestFocus();
        search.clear();
        status.getSelectionModel().selectFirst();
    }

    @FXML private void upload() { Views.prototype(files, "Tải tệp lên"); }
    @FXML private void download() {
        Views.prototype(files, "Tải xuống " + files.getSelectionModel().getSelectedItem().name());
    }
    @FXML private void sync() {
        Views.prototype(files, "Đồng bộ " + files.getSelectionModel().getSelectedItem().name());
    }
}
