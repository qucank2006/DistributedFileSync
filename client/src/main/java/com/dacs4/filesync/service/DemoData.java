package com.dacs4.filesync.service;

import com.dacs4.filesync.model.DemoRecords.*;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;

import java.util.List;
import java.util.Map;

import static com.dacs4.filesync.model.DemoRecords.SyncStatus.*;

public final class DemoData {
    private static final String SERVER = "Máy chủ phòng lab";
    private static final String LAPTOP = "Máy tính của An";
    private static final String STUDIO = "Máy trạm thiết kế";
    public static final String LAST_SYNC = "09:42 · 19/11/2026";

    private static final List<FileItem> FILES = List.of(
        new FileItem("Bao-cao-DACS4.docx", "DOCX", "824 KB", "19/11/2026 09:48", CONFLICT),
        new FileItem("Ke-hoach-tuan.xlsx", "XLSX", "146 KB", "19/11/2026 09:43", CONFLICT),
        new FileItem("So-do-he-thong.pdf", "PDF", "1,8 MB", "19/11/2026 09:40", SYNCED),
        new FileItem("Anh-giao-dien.png", "PNG", "2,4 MB", "19/11/2026 08:52", PENDING),
        new FileItem("Bien-ban-hop.pdf", "PDF", "432 KB", "18/11/2026 16:25", SYNCED),
        new FileItem("Du-lieu-kiem-thu.csv", "CSV", "96 KB", "18/11/2026 15:19", SYNCED),
        new FileItem("Danh-sach-thiet-bi.xlsx", "XLSX", "68 KB", "18/11/2026 14:08", SYNCED),
        new FileItem("Huong-dan-cai-dat.docx", "DOCX", "312 KB", "18/11/2026 10:37", SYNCED),
        new FileItem("Ban-ve-mang.png", "PNG", "916 KB", "17/11/2026 16:04", SYNCED),
        new FileItem("Tai-lieu-tham-khao.zip", "ZIP", "12,6 MB", "17/11/2026 14:31", PENDING),
        new FileItem("Nhat-ky-nhom.txt", "TXT", "24 KB", "17/11/2026 11:50", SYNCED),
        new FileItem("Trinh-bay-tien-do.pptx", "PPTX", "5,2 MB", "16/11/2026 09:14", SYNCED)
    );

    private static final List<Device> DEVICES = List.of(
        new Device(SERVER, "192.168.1.10", true, "19/11/2026 09:50", "Ethernet"),
        new Device(LAPTOP, "192.168.1.24", true, "19/11/2026 09:50", "Wi-Fi"),
        new Device(STUDIO, "192.168.1.35", true, "19/11/2026 09:49", "Ethernet"),
        new Device("Máy tính của Linh", "192.168.1.42", false, "18/11/2026 17:21", "Wi-Fi")
    );

    private static final List<Conflict> CONFLICTS = List.of(
        new Conflict(FILES.get(0),
            new Version("v4", "19/11/2026 09:48", LAPTOP, "824 KB", "d432eae191f7…"),
            new Version("v4", "19/11/2026 09:45", SERVER, "812 KB", "a32b7faca801…")),
        new Conflict(FILES.get(1),
            new Version("v3", "19/11/2026 09:43", STUDIO, "146 KB", "7f327c591898…"),
            new Version("v3", "19/11/2026 09:41", SERVER, "142 KB", "fc893c139107…"))
    );

    private static final Map<FileItem, List<Version>> HISTORIES = Map.of(
        FILES.get(0), List.of(
            new Version("v3", "19/11/2026 09:12", SERVER, "801 KB", "09cb3c821e109e8ed1f9b2a9d30494864293c621e6d65906f03dfb7674789392"),
            new Version("v2", "18/11/2026 14:10", LAPTOP, "760 KB", "5ceb52a9f631c9d27bf4745676c3312fad46a6baa3ed135eb46892478ca7e86a"),
            new Version("v1", "17/11/2026 16:35", LAPTOP, "697 KB", "172b53e1a49cde06dbccb54764bbb97492de718cdca5ac9ce5c0f3b11e1f1f74")),
        FILES.get(1), List.of(
            new Version("v2", "18/11/2026 14:30", STUDIO, "130 KB", "fa410a7be73be1ec7d9e33ec48651623de8061683fdff4f40bb20dde77693d8f"),
            new Version("v1", "17/11/2026 11:21", SERVER, "116 KB", "5facb871da70ecba571c025687cb17c1baaedca4c2350d5aee1064776a2f4965")),
        FILES.get(2), List.of(
            new Version("v2", "19/11/2026 09:40", STUDIO, "1,8 MB", "239beaeb922a1be6834e5212b25544bcdfb0d3566f4f46e314624da80859f1fb"),
            new Version("v1", "18/11/2026 16:24", STUDIO, "1,6 MB", "bd7210c6ca31f13eb16dcaf463e8ac4e7627c94d7d5a931a2cf18634419c4776"))
    );

    private static final List<Activity> ACTIVITIES = List.of(
        new Activity("09:48", "Phát hiện xung đột ở Bao-cao-DACS4.docx", LAPTOP),
        new Activity("09:43", "Phát hiện xung đột ở Ke-hoach-tuan.xlsx", STUDIO),
        new Activity("09:42", "Đã đồng bộ So-do-he-thong.pdf", SERVER),
        new Activity("08:52", "Thêm Anh-giao-dien.png vào hàng đợi", STUDIO)
    );

    public ObservableList<FileItem> files() { return FXCollections.observableArrayList(FILES); }
    public ObservableList<Device> devices() { return FXCollections.observableArrayList(DEVICES); }
    public ObservableList<Conflict> conflicts() { return FXCollections.observableArrayList(CONFLICTS); }
    public ObservableList<Activity> activities() { return FXCollections.observableArrayList(ACTIVITIES); }

    public ObservableList<Version> versions(FileItem file) {
        return FXCollections.observableArrayList(
            file == null ? List.of() : HISTORIES.getOrDefault(file, List.of()));
    }

    public long fileCount(SyncStatus status) {
        return FILES.stream().filter(file -> file.status() == status).count();
    }

    public long onlineCount() {
        return DEVICES.stream().filter(Device::online).count();
    }
}
