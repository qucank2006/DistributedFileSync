package com.dacs4.filesync.controller;

import com.dacs4.filesync.model.DemoRecords.*;
import com.dacs4.filesync.service.DemoData;
import com.dacs4.filesync.util.Views;
import javafx.fxml.FXML;
import javafx.scene.control.*;
import javafx.scene.input.Clipboard;
import javafx.scene.input.ClipboardContent;

public final class HistoryController {
    @FXML private ComboBox<FileItem> file;
    @FXML private TableView<Version> versions;
    @FXML private Button restore, copyChecksum;
    @FXML private Label selectedDetails, copyStatus;
    @FXML private TextField checksum;

    @FXML private void initialize() {
        DemoData data = new DemoData();
        Views.table(versions, "Chưa có lịch sử phiên bản mẫu cho tệp này.");
        versions.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY_FLEX_NEXT_COLUMN);
        Views.column(versions, "Phiên bản", 96, Version::number).setPrefWidth(108);
        Views.column(versions, "Thời điểm sửa đổi", 158, Version::modified).setSortable(false);
        Views.column(versions, "Thiết bị nguồn", 165, Version::device).setPrefWidth(210);
        var size = Views.column(versions, "Dung lượng", 100, Version::size);
        size.setSortable(false);
        size.getStyleClass().add("numeric-column");

        var selection = versions.getSelectionModel().selectedItemProperty();
        restore.disableProperty().bind(selection.isNull());
        copyChecksum.disableProperty().bind(selection.isNull());
        checksum.disableProperty().bind(selection.isNull());
        selection.addListener((property, before, selected) -> {
            selectedDetails.setText(selected == null ? "Chọn một phiên bản để xem chi tiết."
                : "Phiên bản " + selected.number() + " · " + selected.modified() + " · " + selected.device());
            checksum.setText(selected == null ? "" : selected.checksum());
            checksum.positionCaret(0);
            copyStatus.setText("");
        });
        file.setItems(data.files());
        file.valueProperty().addListener((property, before, selected) -> {
            versions.getSelectionModel().clearSelection();
            versions.setItems(data.versions(selected));
        });
        file.getSelectionModel().selectFirst();
    }

    @FXML private void copyChecksum() {
        ClipboardContent content = new ClipboardContent();
        content.putString(checksum.getText());
        boolean copied = Clipboard.getSystemClipboard().setContent(content);
        copyStatus.setText(copied ? "Đã sao chép." : "Chưa sao chép được.");
    }

    @FXML private void restore() {
        Version selected = versions.getSelectionModel().getSelectedItem();
        Views.prototype(versions, "Khôi phục " + file.getValue().name() + " · " + selected.number());
    }
}
