package com.dacs4.filesync.model;

public final class DemoRecords {
    private DemoRecords() {}

    public enum SyncStatus {
        SYNCED("Đã đồng bộ", "success"),
        PENDING("Đang chờ", "warning"),
        CONFLICT("Xung đột", "danger");

        private final String label;
        private final String style;

        SyncStatus(String label, String style) {
            this.label = label;
            this.style = style;
        }

        public String label() { return label; }
        public String style() { return style; }
    }

    public record FileItem(String name, String type, String size, String modified, SyncStatus status) {
        @Override public String toString() { return name; }
    }

    public record Version(String number, String modified, String device, String size, String checksum) {}

    public record Conflict(FileItem file, Version local, Version server) {
        @Override public String toString() { return file.name(); }
    }

    public record Device(String name, String ip, boolean online, String lastSeen, String connectionType) {}

    public record Activity(String time, String description, String device) {}
}
