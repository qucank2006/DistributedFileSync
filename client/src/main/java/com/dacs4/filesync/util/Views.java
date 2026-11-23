package com.dacs4.filesync.util;

import java.util.Arrays;
import java.util.function.Function;

import javafx.beans.property.ReadOnlyObjectWrapper;
import javafx.scene.Node;
import javafx.scene.control.*;
import javafx.scene.shape.SVGPath;

public final class Views {
    private Views() {}

    public static <T, V> TableColumn<T, V> column(TableView<T> table, String title,
                                                  double minWidth, Function<T, V> value) {
        TableColumn<T, V> column = new TableColumn<>(title);
        column.setMinWidth(minWidth);
        column.setCellValueFactory(cell -> new ReadOnlyObjectWrapper<>(value.apply(cell.getValue())));
        table.getColumns().add(column);
        return column;
    }

    public static <T, V> void badges(TableColumn<T, V> column, Function<V, String> label,
                                     Function<V, String> style) {
        column.setCellFactory(ignored -> new TableCell<>() {
            @Override
            protected void updateItem(V value, boolean empty) {
                super.updateItem(value, empty);
                setGraphic(empty || value == null ? null : badge(label.apply(value), style.apply(value)));
            }
        });
    }

    public static Label badge(String text, String style) {
        Label label = new Label(text);
        label.getStyleClass().addAll("badge", style);
        return label;
    }

    public static void table(TableView<?> table, String emptyMessage) {
        table.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY_FLEX_LAST_COLUMN);
        table.setPlaceholder(new Label(emptyMessage));
    }

    public static Tooltip fullText(String text) {
        var tooltip = new Tooltip(text);
        tooltip.setWrapText(true);
        tooltip.setMaxWidth(460);
        return tooltip;
    }

    public static String filenameFirst(String path) {
        int slash = path.lastIndexOf('/');
        return slash < 0 ? path : path.substring(slash + 1) + " · " + path.substring(0, slash);
    }

    public static <T> void readableChoices(ComboBox<T> box, Function<T, String> display, Function<T, String> full) {
        java.util.function.Supplier<ListCell<T>> cell = () -> new ListCell<>() {
            @Override protected void updateItem(T item, boolean empty) {
                super.updateItem(item, empty);
                setText(empty || item == null ? null : display.apply(item));
                setTooltip(empty || item == null ? null : fullText(full.apply(item)));
            }
        };
        box.setCellFactory(ignored -> cell.get());
        box.setButtonCell(cell.get());
    }

    public static void icon(Button button, String path) {
        SVGPath graphic = new SVGPath();
        graphic.setContent(path);
        graphic.getStyleClass().add("nav-icon");
        button.setGraphic(graphic);
    }

    public static void prototype(Node owner, String action) {
        Alert dialog = dialog(owner, Alert.AlertType.INFORMATION, action,
            "Thao tác này chỉ minh họa giao diện. Không có dữ liệu hoặc kết nối nào được thay đổi.",
            new ButtonType("Đóng", ButtonBar.ButtonData.OK_DONE));
        dialog.showAndWait();
    }

    public static void message(Node owner, String title, String description) {
        dialog(owner, Alert.AlertType.INFORMATION, title, description,
            new ButtonType("Đóng", ButtonBar.ButtonData.OK_DONE)).showAndWait();
    }

    public static boolean confirm(Node owner, String description) {
        ButtonType confirm = new ButtonType("Xác nhận", ButtonBar.ButtonData.OK_DONE);
        Alert dialog = dialog(owner, Alert.AlertType.CONFIRMATION, "Xác nhận lựa chọn", description,
            confirm, new ButtonType("Hủy", ButtonBar.ButtonData.CANCEL_CLOSE));
        return dialog.showAndWait().filter(confirm::equals).isPresent();
    }

    private static Alert dialog(Node owner, Alert.AlertType type, String title, String description,
                                ButtonType... buttons) {
        Alert alert = new Alert(type, description, buttons);
        alert.initOwner(owner.getScene().getWindow());
        alert.setTitle("Bản mẫu DACS4");
        alert.setHeaderText(title);
        alert.getDialogPane().getStylesheets().addAll(owner.getScene().getRoot().getStylesheets());
        alert.getDialogPane().setGraphic(null);
        return alert;
    }

    public static boolean validIpv4(String address) {
        String[] parts = address.split("\\.", -1);
        return parts.length == 4 && Arrays.stream(parts)
            .allMatch(part -> part.matches("[0-9]{1,3}") && Integer.parseInt(part) <= 255);
    }
}
