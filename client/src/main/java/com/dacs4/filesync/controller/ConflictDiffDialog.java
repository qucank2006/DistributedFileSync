package com.dacs4.filesync.controller;

import com.dacs4.filesync.service.ConflictComparison;
import com.dacs4.filesync.service.LineDiff;
import com.dacs4.filesync.service.LineDiff.Row;
import com.dacs4.filesync.util.Views;
import javafx.beans.property.ReadOnlyStringWrapper;
import javafx.css.PseudoClass;
import javafx.geometry.Pos;
import javafx.scene.control.*;
import javafx.scene.layout.*;
import javafx.stage.Modality;
import javafx.stage.Window;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

final class ConflictDiffDialog extends Dialog<ButtonType> {
    private static final PseudoClass ADDED = PseudoClass.getPseudoClass("diff-added");
    private static final PseudoClass REMOVED = PseudoClass.getPseudoClass("diff-removed");
    private static final PseudoClass MODIFIED = PseudoClass.getPseudoClass("diff-modified");

    private final AtomicBoolean cancelled = new AtomicBoolean();
    private final TableView<Row> table = new TableView<>();
    private final Label status = new Label("Đang chuẩn bị bản chụp để so sánh…");
    private final Label position = new Label();
    private final Button previous = new Button("Khác biệt trước");
    private final Button next = new Button("Khác biệt sau");
    private final TextArea localHash = lineDetails("SHA-256 cục bộ");
    private final TextArea serverHash = lineDetails("SHA-256 máy chủ");
    private final TextArea localLine = lineDetails("Dòng cục bộ");
    private final TextArea serverLine = lineDetails("Dòng máy chủ");
    private List<Integer> differences = List.of();
    private int current = -1;

    ConflictDiffDialog(Window owner, List<String> stylesheets, String path) {
        initOwner(owner);
        initModality(Modality.NONE);
        setTitle("So sánh nội dung tệp");
        setResizable(true);
        var pane = getDialogPane();
        pane.getStylesheets().addAll(stylesheets);
        pane.setPrefSize(990, 660);
        pane.setMinWidth(720);
        pane.getButtonTypes().add(new ButtonType("Đóng", ButtonBar.ButtonData.CANCEL_CLOSE));

        var file = readout("Đường dẫn tệp");
        file.setText(path);
        var pathLabel = new Label("Đường dẫn");
        pathLabel.setLabelFor(file);
        HBox.setHgrow(file, Priority.ALWAYS);
        var pathRow = new HBox(10, pathLabel, file);
        pathRow.setAlignment(Pos.CENTER_LEFT);
        localHash.getStyleClass().add("diff-hash");
        serverHash.getStyleClass().add("diff-hash");

        configureTable();
        status.setId("diffStatus");
        status.setWrapText(true);
        var navigation = new HBox(8, previous, next, position);
        previous.setId("diffPrevious");
        next.setId("diffNext");
        navigation.setAlignment(Pos.CENTER_LEFT);
        previous.setOnAction(event -> jump(current - 1));
        next.setOnAction(event -> jump(current + 1));
        updateNavigation();

        var note = new Label("So sánh hai bản chụp theo dòng. Kết quả chỉ thể hiện sự khác nhau giữa hai bản.");
        note.getStyleClass().add("muted-text");
        note.setWrapText(true);
        VBox.setVgrow(table, Priority.ALWAYS);
        var body = new VBox(10, pathRow, pair("SHA-256 cục bộ", localHash, "SHA-256 máy chủ", serverHash),
            note, status, navigation, table, pair("Dòng cục bộ", localLine, "Dòng máy chủ", serverLine));
        body.getStyleClass().add("diff-content");
        pane.setContent(body);
    }

    private void configureTable() {
        table.setId("diffTable");
        table.getStyleClass().add("diff-table");
        table.setPlaceholder(new Label());
        table.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY_FLEX_NEXT_COLUMN);
        var change = column("Thay đổi", 82, row -> switch (row.change()) {
            case SAME -> "";
            case ADDED -> "Thêm";
            case REMOVED -> "Xóa";
            case MODIFIED -> "Sửa";
        });
        change.setMaxWidth(92);
        table.getColumns().add(change);
        addSide("Bản cục bộ", row -> row.localNumber() == 0 ? "" : Integer.toString(row.localNumber()), Row::local);
        addSide("Bản máy chủ", row -> row.serverNumber() == 0 ? "" : Integer.toString(row.serverNumber()), Row::server);
        table.setRowFactory(ignored -> new TableRow<>() {
            @Override protected void updateItem(Row row, boolean empty) {
                super.updateItem(row, empty);
                var changeType = row == null ? LineDiff.Change.SAME : row.change();
                pseudoClassStateChanged(ADDED, !empty && changeType == LineDiff.Change.ADDED);
                pseudoClassStateChanged(REMOVED, !empty && changeType == LineDiff.Change.REMOVED);
                pseudoClassStateChanged(MODIFIED, !empty && changeType == LineDiff.Change.MODIFIED);
            }
        });
        table.getSelectionModel().selectedItemProperty().addListener((property, before, row) -> {
            localLine.setText(row == null ? "" : row.local());
            serverLine.setText(row == null ? "" : row.server());
            localLine.positionCaret(0);
            serverLine.positionCaret(0);
        });
    }

    private void addSide(String title, Function<Row, String> lineNumber, Function<Row, String> text) {
        var side = new TableColumn<Row, String>(title);
        side.setReorderable(false);
        var number = column("Dòng", 56, lineNumber);
        number.setMaxWidth(64);
        number.getStyleClass().add("numeric-column");
        var content = column("Nội dung", 170, text);
        content.getStyleClass().add("diff-code");
        side.getColumns().add(number);
        side.getColumns().add(content);
        table.getColumns().add(side);
    }

    private static TableColumn<Row, String> column(String title, double minimum, Function<Row, String> value) {
        var column = new TableColumn<Row, String>(title);
        column.setMinWidth(minimum);
        column.setSortable(false);
        column.setReorderable(false);
        column.setCellValueFactory(cell -> new ReadOnlyStringWrapper(value.apply(cell.getValue())));
        return column;
    }

    private static HBox pair(String leftLabel, Control left, String rightLabel, Control right) {
        return new HBox(10, labelled(leftLabel, left), labelled(rightLabel, right));
    }

    private static VBox labelled(String title, Control control) {
        var caption = new Label(title);
        caption.setLabelFor(control);
        caption.getStyleClass().add("muted-text");
        var box = new VBox(4, caption, control);
        box.setMinWidth(0);
        box.setPrefWidth(0);
        HBox.setHgrow(box, Priority.ALWAYS);
        return box;
    }

    private static TextField readout(String name) {
        var value = new TextField();
        value.setAccessibleText(name);
        value.setEditable(false);
        value.setMinWidth(0);
        value.getStyleClass().add("readout");
        return value;
    }

    private static TextArea lineDetails(String name) {
        var value = new TextArea();
        value.setAccessibleText(name);
        value.setEditable(false);
        value.setWrapText(true);
        value.setPrefRowCount(2);
        value.setMinWidth(0);
        value.getStyleClass().add("readout");
        return value;
    }

    boolean cancelled() {
        return cancelled.get();
    }

    void cancelCapture() {
        cancelled.set(true);
    }

    void showResult(ConflictComparison.Result result) {
        localHash.setText(result.localSha256());
        serverHash.setText(result.serverSha256());
        table.getItems().setAll(result.diff().rows());
        differences = result.diff().differences();
        long changedRows = result.diff().rows().stream().filter(row -> row.change() != LineDiff.Change.SAME).count();
        status.setText(differences.isEmpty() ? "Không có khác biệt theo dòng."
            : differences.size() + " vùng khác biệt · " + changedRows + " dòng so sánh có thay đổi");
        jump(0);
    }

    void showFailure(String message) {
        cancelCapture();
        differences = List.of();
        current = -1;
        table.getItems().clear();
        localHash.clear();
        serverHash.clear();
        status.setText(message);
        status.getStyleClass().add("warning-text");
        updateNavigation();
    }

    private void jump(int index) {
        if (index >= 0 && index < differences.size()) {
            current = index;
            int row = differences.get(index);
            table.getSelectionModel().select(row);
            table.scrollTo(row);
        }
        updateNavigation();
    }

    private void updateNavigation() {
        previous.setDisable(current <= 0);
        next.setDisable(current + 1 >= differences.size());
        position.setText(differences.isEmpty() ? "" : (current + 1) + " / " + differences.size());
    }
}
