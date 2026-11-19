package com.dacs4.filesync.shared.protocol;

import com.dacs4.filesync.shared.exception.ProtocolException;

public enum MessageType {
    HELLO(1), WELCOME(2), PING(3), PONG(4), DISCONNECT(5), ERROR(6);

    private final int code;

    MessageType(int code) {
        this.code = code;
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
