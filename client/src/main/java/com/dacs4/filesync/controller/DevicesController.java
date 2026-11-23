package com.dacs4.filesync.controller;

import com.dacs4.filesync.util.Views;
import com.dacs4.filesync.service.ConnectionService;
import com.dacs4.filesync.network.ConnectionState;
import javafx.fxml.FXML;
import javafx.scene.control.*;
import javafx.css.PseudoClass;
import javafx.beans.binding.Bindings;

public final class DevicesController {
    @FXML private TextField ip, port, sessionId;
    @FXML private Button connect, disconnect;
    @FXML private Label ipError, portError, connectionDetail, sessionEmpty, endpoint;
    @FXML private javafx.scene.layout.GridPane sessionDetails;
    private ConnectionService connection;

    private static final PseudoClass INVALID = PseudoClass.getPseudoClass("invalid");
    private static final String IP_ERROR = "Nhập IPv4 hợp lệ, ví dụ 192.168.1.10.";
    private static final String PORT_ERROR = "Dùng cổng 1–65535.";

    @FXML private void initialize() {
        ipError.visibleProperty().bind(ipError.textProperty().isNotEmpty());
        ipError.managedProperty().bind(ipError.visibleProperty());
        portError.visibleProperty().bind(portError.textProperty().isNotEmpty());
        portError.managedProperty().bind(portError.visibleProperty());
        sessionDetails.managedProperty().bind(sessionDetails.visibleProperty());
        sessionEmpty.visibleProperty().bind(sessionDetails.visibleProperty().not());
        sessionEmpty.managedProperty().bind(sessionEmpty.visibleProperty());
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
    }

    void bindConnection(ConnectionService service) {
        connection = service;
        var status = service.statusProperty();
        var active = Bindings.createBooleanBinding(() -> status.get().state().active(), status);
        connect.disableProperty().bind(active);
        disconnect.disableProperty().bind(active.not());
        connect.visibleProperty().bind(active.not());
        disconnect.visibleProperty().bind(active);
        connect.managedProperty().bind(connect.visibleProperty());
        disconnect.managedProperty().bind(disconnect.visibleProperty());
        ip.disableProperty().bind(active);
        port.disableProperty().bind(active);
        connectionDetail.textProperty().bind(Bindings.createStringBinding(() -> {
            var current = status.get();
            return current.state() == ConnectionState.CONNECTED ? ""
                : current.detail();
        }, status));
        connectionDetail.visibleProperty().bind(connectionDetail.textProperty().isNotEmpty());
        connectionDetail.managedProperty().bind(connectionDetail.visibleProperty());
        status.addListener((property, before, after) -> {
            connectionDetail.getStyleClass().setAll("label",
                after.state() == ConnectionState.ERROR ? "danger-text" : "muted-text");
            refreshSession();
            if (after.state() == ConnectionState.DISCONNECTED && ip.getScene() != null) {
                connect.requestFocus();
            }
        });
        refreshSession();
    }

    private void refreshSession() {
        var client = connection.connectedClient();
        sessionDetails.setVisible(client != null);
        if (client != null) {
            var address = client.serverEndpoint();
            endpoint.setText(address.getHostString() + ":" + address.getPort());
            sessionId.setText(client.debugSessionId());
        } else {
            sessionId.clear();
            endpoint.setText("");
        }
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
