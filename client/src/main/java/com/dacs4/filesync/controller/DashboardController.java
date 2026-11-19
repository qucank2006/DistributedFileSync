package com.dacs4.filesync.controller;

import com.dacs4.filesync.model.DemoRecords.*;
import com.dacs4.filesync.service.DemoData;
import com.dacs4.filesync.util.Views;
import javafx.fxml.FXML;
import javafx.scene.control.*;

public final class DashboardController {
    @FXML private Label onlineCount, syncedCount, pendingCount, lastSync, conflictCount;
    @FXML private TableView<Activity> activities;
    @FXML private Button openConflicts;

    @FXML private void initialize() {
        DemoData data = new DemoData();
        onlineCount.setText(data.onlineCount() + " / " + data.devices().size());
        syncedCount.setText(Long.toString(data.fileCount(SyncStatus.SYNCED)));
        pendingCount.setText(Long.toString(data.fileCount(SyncStatus.PENDING)));
        long conflicts = data.fileCount(SyncStatus.CONFLICT);
        conflictCount.setText(Long.toString(conflicts));
        openConflicts.setDisable(conflicts == 0);
        lastSync.setText(DemoData.LAST_SYNC);
        Views.table(activities, "Chưa có hoạt động.");
        Views.column(activities, "Thời gian", 95, Activity::time);
        Views.column(activities, "Hoạt động", 360, Activity::description);
        Views.column(activities, "Thiết bị", 165, Activity::device);
        activities.setItems(data.activities());
    }

    void setOpenConflictsAction(Runnable action) {
        openConflicts.setOnAction(event -> action.run());
    }
}
