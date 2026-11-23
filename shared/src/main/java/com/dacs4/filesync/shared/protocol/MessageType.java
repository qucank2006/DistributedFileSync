package com.dacs4.filesync.shared.protocol;

import com.dacs4.filesync.shared.exception.ProtocolException;

public enum MessageType {
    HELLO(1), WELCOME(2), PING(3), PONG(4), DISCONNECT(5), ERROR(6),
    UPLOAD_INIT(7), UPLOAD_READY(8), FILE_CHUNK(9), UPLOAD_END(10), UPLOAD_RESULT(11),
    DOWNLOAD_REQUEST(12), DOWNLOAD_INFO(13), DOWNLOAD_END(14), DOWNLOAD_RESULT(15),
    FILE_LIST_REQUEST(16), FILE_LIST_RESPONSE(17), FILE_CHANGED(18),
    VERSION_LIST_REQUEST(19), VERSION_LIST_RESPONSE(20), RESTORE_REQUEST(21), RESTORE_RESPONSE(22);

    private final int code;

    MessageType(int code) {
        this.code = code;
    }

    public boolean isFileTransfer() {
        return (code >= UPLOAD_INIT.code && code <= FILE_LIST_RESPONSE.code)
            || this == VERSION_LIST_REQUEST || this == VERSION_LIST_RESPONSE
            || this == RESTORE_REQUEST || this == RESTORE_RESPONSE;
    }

    public boolean isBinary() {
        return isFileTransfer() || this == FILE_CHANGED;
    }

    int code() {
        return code;
    }

    static MessageType fromCode(int code) throws ProtocolException {
        for (MessageType type : values()) {
            if (type.code == code) {
                return type;
            }
        }
        throw new ProtocolException("Unknown message type");
    }
}
