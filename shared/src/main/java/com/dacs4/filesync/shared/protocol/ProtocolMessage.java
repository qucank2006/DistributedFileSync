package com.dacs4.filesync.shared.protocol;

import java.util.Objects;

/**
 * HELLO carries a client name, WELCOME a session UUID and ERROR a description.
 * PING, PONG and DISCONNECT carry an empty payload.
 */
public record ProtocolMessage(MessageType type, String payload) {
    public ProtocolMessage {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(payload, "payload");
    }

    public ProtocolMessage(MessageType type) {
        this(type, "");
    }
}
