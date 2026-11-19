package com.dacs4.filesync.controller;

import com.dacs4.filesync.model.DemoRecords.*;
import com.dacs4.filesync.service.DemoData;
import com.dacs4.filesync.util.Views;
import javafx.fxml.FXML;
import javafx.scene.control.*;

public final class ConflictsController {
    @FXML private ComboBox<Conflict> conflict;
    @FXML private Label localVersion, localTime, localDevice, localSize;
    @FXML private Label serverVersion, serverTime, serverDevice, serverSize;
    @FXML private Label preview;
    @FXML private Button keepLocalButton, keepServerButton, keepBothButton;

    @FXML private void initialize() {
        conflict.setItems(new DemoData().conflicts());
        var noConflict = conflict.valueProperty().isNull();
        keepLocalButton.disableProperty().bind(noConflict);
        keepServerButton.disableProperty().bind(noConflict);
        keepBothButton.disableProperty().bind(noConflict);
        conflict.valueProperty().addListener((property, before, selected) -> {
            display(selected == null ? null : selected.local(), localVersion, localTime, localDevice, localSize);
            display(selected == null ? null : selected.server(), serverVersion, serverTime, serverDevice, serverSize);
            preview.setText(selected == null ? "Chưa chọn tệp xung đột." : "");
        });
        display(null, localVersion, localTime, localDevice, localSize);
        display(null, serverVersion, serverTime, serverDevice, serverSize);
        conflict.getSelectionModel().selectFirst();
    }

    private void display(Version version, Label number, Label time, Label device, Label size) {
        number.setText(version == null ? "—" : version.number());
        time.setText(version == null ? "—" : version.modified());
        device.setText(version == null ? "—" : version.device());
        size.setText(version == null ? "—" : version.size());
    }

    @FXML private void keepLocal() { choose("Giữ bản cục bộ"); }
    @FXML private void keepServer() { choose("Giữ bản máy chủ"); }
    @FXML private void keepBoth() { choose("Giữ cả hai"); }

    private void choose(String action) {
        Conflict selected = conflict.getValue();
        if (selected == null) {
            return;
        }
        String file = selected.file().name();
        if (Views.confirm(conflict, action + " cho " + file + "? Đây là lựa chọn minh họa.")) {
            preview.setText("Đã chọn “" + action + "” cho " + file + ". Dữ liệu mẫu không thay đổi.");
        }
    }
}
