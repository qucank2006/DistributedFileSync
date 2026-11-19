package com.dacs4.filesync.network;

public enum ConnectionState {
    CONNECTING("Đang kết nối", "info"),
    CONNECTED("Đã kết nối", "success"),
    DISCONNECTED("Chưa kết nối", "muted"),
    RECONNECTING("Đang kết nối lại", "warning"),
    ERROR("Lỗi kết nối", "danger");

    private final String label;
    private final String style;

    ConnectionState(String label, String style) {
        this.label = label;
        this.style = style;
    }

    public String label() {
        return label;
    }

    public String style() {
        return style;
    }

    public boolean active() {
        return this == CONNECTING || this == CONNECTED || this == RECONNECTING;
    }
}
