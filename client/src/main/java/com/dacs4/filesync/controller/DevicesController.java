package com.dacs4.filesync.controller;

import com.dacs4.filesync.model.DemoRecords.*;
import com.dacs4.filesync.service.DemoData;
import com.dacs4.filesync.util.Views;
import com.dacs4.filesync.service.ConnectionService;
import javafx.fxml.FXML;
import javafx.scene.control.*;
import javafx.css.PseudoClass;
import javafx.beans.binding.Bindings;

public final class DevicesController {
    @FXML private TableView<Device> devices;
    @FXML private TextField ip, port;
    @FXML private Button connect, disconnect;
    @FXML private Label ipError, portError, selectedDevice, connectionDetail;
    private ConnectionService connection;

    private static final PseudoClass INVALID = PseudoClass.getPseudoClass("invalid");
    private static final String IP_ERROR = "Nhập IPv4 hợp lệ, ví dụ 192.168.1.10.";
    private static final String PORT_ERROR = "Dùng cổng 1–65535.";

    @FXML private void initialize() {
        Views.table(devices, "Chưa có thiết bị mẫu.");
        Views.column(devices, "Thiết bị", 158, Device::name);
        Views.column(devices, "Địa chỉ IP", 120, Device::ip);
        Views.column(devices, "Lần cuối xuất hiện", 150, Device::lastSeen).setSortable(false);
        Views.column(devices, "Loại kết nối", 100, Device::connectionType);
        devices.setItems(new DemoData().devices());
        var selected = devices.getSelectionModel().selectedItemProperty();
        selectedDevice.textProperty().bind(Bindings.createStringBinding(
            () -> selected.get() == null ? "Chưa chọn thiết bị" : "Thiết bị đã chọn · " + selected.get().name(), selected));
        ip.textProperty().addListener((observable, before, value) -> {
            if (!ipError.getText().isEmpty()) {
                setError(ip, ipError, Views.validIpv4(value.strip()) ? "" : IP_ERROR);
            }
        });
        port.textProperty().addListener((observable, before, value) -> {
            if (!portError.getText().isEmpty()) {
                setError(port, portError, validPort(value.strip()) ? "" : PORT_ERROR);
            }
        });
        devices.getSelectionModel().selectFirst();
    }

    void bindConnection(ConnectionService service) {
        connection = service;
        var status = service.statusProperty();
        var active = Bindings.createBooleanBinding(() -> status.get().state().active(), status);
        connect.disableProperty().bind(active);
        disconnect.disableProperty().bind(active.not());
        ip.disableProperty().bind(active);
        port.disableProperty().bind(active);
        connectionDetail.textProperty().bind(Bindings.createStringBinding(() -> {
            var current = status.get();
            return current.detail().isEmpty() ? current.state().label() : current.detail();
        }, status));
    }

    @FXML private void disconnect() {
        connection.disconnect();
    }

    @FXML private void connectServer() {
        String address = ip.getText().strip();
        String portValue = port.getText().strip();
        boolean addressValid = Views.validIpv4(address);
        boolean portValid = validPort(portValue);
        setError(ip, ipError, addressValid ? "" : IP_ERROR);
        setError(port, portError, portValid ? "" : PORT_ERROR);
        if (!addressValid) {
            ip.requestFocus();
        } else if (!portValid) {
            port.requestFocus();
        } else {
            connection.connect(address, Integer.parseInt(portValue));
        }
    }

    private static boolean validPort(String value) {
        if (!value.matches("[0-9]{1,5}")) {
            return false;
        }
        int number = Integer.parseInt(value);
        return number >= 1 && number <= 65535;
    }

    private static void setError(TextField field, Label label, String message) {
        label.setText(message);
        field.setAccessibleHelp(message.isEmpty() ? null : message);
        field.pseudoClassStateChanged(INVALID, !message.isEmpty());
    }
}
