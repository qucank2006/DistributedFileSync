package com.dacs4.filesync.shared.protocol;

import com.dacs4.filesync.shared.exception.ProtocolException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;

/** Big-endian length of the body, then one version byte, one type byte and a type-specific payload. */
public final class MessageCodec {
    public static final int VERSION = 1;
    public static final int MAX_MESSAGE_BYTES = 1024 * 1024;
    private static final int HEADER_BYTES = 2;

    private MessageCodec() {}

    /** Returns null only for EOF between complete frames. */
    public static ProtocolMessage read(DataInputStream input) throws IOException {
        int first = input.read();
        if (first == -1) {
            return null;
        }
        try {
            int length = (first << 24) | (input.readUnsignedByte() << 16)
                | (input.readUnsignedByte() << 8) | input.readUnsignedByte();
            if (length < HEADER_BYTES || length > MAX_MESSAGE_BYTES) {
                throw new ProtocolException("Invalid frame length");
            }
            if (input.readUnsignedByte() != VERSION) {
                throw new ProtocolException("Unsupported protocol version");
            }
            MessageType type = MessageType.fromCode(input.readUnsignedByte());
            byte[] bytes = new byte[length - HEADER_BYTES];
            input.readFully(bytes);
            if (type.isFileTransfer()) {
                return new ProtocolMessage(type, bytes);
            }
            String payload = StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes)).toString();
            validatePayload(type, payload);
            return new ProtocolMessage(type, payload);
        } catch (EOFException exception) {
            throw new ProtocolException("Incomplete frame", exception);
        } catch (CharacterCodingException exception) {
            throw new ProtocolException("Invalid UTF-8 payload", exception);
        }
    }

    public static void write(DataOutputStream output, ProtocolMessage message) throws IOException {
        if (message.type().isFileTransfer()) {
            writeFrame(output, message.type(), message.binaryPayload());
            return;
        }
        validatePayload(message.type(), message.payload());
        if (message.payload().length() > MAX_MESSAGE_BYTES - HEADER_BYTES) {
            throw new ProtocolException("Message too large");
        }
        ByteBuffer utf8;
        try {
            utf8 = StandardCharsets.UTF_8.newEncoder().encode(CharBuffer.wrap(message.payload()));
        } catch (CharacterCodingException exception) {
            throw new ProtocolException("Invalid text payload", exception);
        }
        byte[] payload = new byte[utf8.remaining()];
        utf8.get(payload);
        writeFrame(output, message.type(), payload);
    }

    private static void writeFrame(DataOutputStream output, MessageType type, byte[] payload) throws IOException {
        if (payload.length > MAX_MESSAGE_BYTES - HEADER_BYTES) {
            throw new ProtocolException("Message too large");
        }
        output.writeInt(HEADER_BYTES + payload.length);
        output.writeByte(VERSION);
        output.writeByte(type.code());
        output.write(payload);
        output.flush();
    }

    private static void validatePayload(MessageType type, String payload) throws ProtocolException {
        boolean valid = switch (type) {
            case HELLO -> !payload.isBlank() && payload.length() <= 128
                && payload.codePoints().noneMatch(Character::isISOControl);
            case WELCOME, ERROR -> !payload.isBlank();
            case PING, PONG, DISCONNECT -> payload.isEmpty();
            default -> false;
        };
        if (!valid) {
            throw new ProtocolException("Invalid payload for " + type);
        }
    }
}
