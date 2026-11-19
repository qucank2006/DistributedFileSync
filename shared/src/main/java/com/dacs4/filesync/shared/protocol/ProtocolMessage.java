package com.dacs4.filesync.shared.protocol;

import java.util.Objects;

/** Phase 1 uses text. Transfer frames carry a binary payload. */
public record ProtocolMessage(MessageType type, String payload, byte[] binaryPayload) {
    public ProtocolMessage {
        Objects.requireNonNull(type);
        if (type.isFileTransfer() ? payload != null || binaryPayload == null
                                  : payload == null || binaryPayload != null) {
            throw new IllegalArgumentException("Payload does not match message type");
        }
    }

    public ProtocolMessage(MessageType type, String payload) {
        this(type, payload, null);
    }

    public ProtocolMessage(MessageType type, byte[] binaryPayload) {
        this(type, null, binaryPayload);
    }

    public ProtocolMessage(MessageType type) {
        this(type, "");
    }
}
