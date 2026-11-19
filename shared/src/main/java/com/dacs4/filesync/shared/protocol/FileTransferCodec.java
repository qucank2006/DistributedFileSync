package com.dacs4.filesync.shared.protocol;

import com.dacs4.filesync.shared.dto.FileMetadata;
import com.dacs4.filesync.shared.exception.ProtocolException;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

/** Transfer envelope: request UUID, transfer UUID (zero for list), then the type-specific bytes. */
public final class FileTransferCodec {
    public static final UUID NONE = new UUID(0, 0);
    public static final int CHUNK_BYTES = 64 * 1024;
    public static final int HASH_BYTES = 32;
    public static final int MAX_LIST_FILES = 1000;
    private static final int MAX_TEXT_BYTES = 512;

    private FileTransferCodec() {}

    public record Packet(MessageType type, UUID requestId, UUID transferId, byte[] data) {
        public DataInputStream body() {
            return new DataInputStream(new ByteArrayInputStream(data));
        }
    }

    @FunctionalInterface
    public interface BodyWriter {
        void write(DataOutputStream output) throws IOException;
    }

    public static ProtocolMessage encode(MessageType type, UUID request, UUID transfer, byte[] data) {
        ByteBuffer bytes = ByteBuffer.allocate(32 + data.length);
        bytes.putLong(request.getMostSignificantBits()).putLong(request.getLeastSignificantBits());
        bytes.putLong(transfer.getMostSignificantBits()).putLong(transfer.getLeastSignificantBits());
        bytes.put(data);
        return new ProtocolMessage(type, bytes.array());
    }

    public static byte[] body(BodyWriter writer) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        writer.write(new DataOutputStream(bytes));
        return bytes.toByteArray();
    }

    public static Packet decode(ProtocolMessage message) throws ProtocolException {
        if (!message.type().isFileTransfer() || message.binaryPayload().length < 32) {
            throw new ProtocolException("Invalid transfer envelope");
        }
        ByteBuffer bytes = ByteBuffer.wrap(message.binaryPayload());
        UUID request = new UUID(bytes.getLong(), bytes.getLong());
        UUID transfer = new UUID(bytes.getLong(), bytes.getLong());
        boolean list = message.type() == MessageType.FILE_LIST_REQUEST
            || message.type() == MessageType.FILE_LIST_RESPONSE;
        if (request.equals(NONE) || (transfer.equals(NONE) && !list) || (list && !transfer.equals(NONE))) {
            throw new ProtocolException("Invalid request or transfer ID");
        }
        byte[] data = new byte[bytes.remaining()];
        bytes.get(data);
        return new Packet(message.type(), request, transfer, data);
    }

    public static void writeText(DataOutputStream output, String text) throws IOException {
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > MAX_TEXT_BYTES) {
            throw new ProtocolException("Transfer text too long");
        }
        output.writeShort(bytes.length);
        output.write(bytes);
    }

    public static String readText(DataInputStream input) throws IOException {
        int length = input.readUnsignedShort();
        if (length > MAX_TEXT_BYTES) {
            throw new ProtocolException("Transfer text too long");
        }
        byte[] bytes = input.readNBytes(length);
        if (bytes.length != length) {
            throw new ProtocolException("Incomplete text");
        }
        return StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes)).toString();
    }

    public static void writeMetadata(DataOutputStream output, FileMetadata file) throws IOException {
        writeText(output, file.name());
        output.writeLong(file.size());
        output.writeLong(file.modifiedMillis());
    }

    public static FileMetadata readMetadata(DataInputStream input) throws IOException {
        String name = readText(input);
        long size = input.readLong();
        long modified = input.readLong();
        if (size < 0 || !validName(name)) {
            throw new ProtocolException("Invalid file metadata");
        }
        return new FileMetadata(name, size, modified);
    }

    public static List<FileMetadata> readList(byte[] data) throws IOException {
        var input = new DataInputStream(new ByteArrayInputStream(data));
        int count = input.readInt();
        if (count < 0 || count > MAX_LIST_FILES) {
            throw new ProtocolException("Invalid file count");
        }
        List<FileMetadata> files = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            files.add(readMetadata(input));
        }
        requireEnd(input);
        return List.copyOf(files);
    }

    public static byte[] result(String error) throws IOException {
        return body(output -> {
            output.writeByte(error == null ? 0 : 1);
            if (error != null) {
                writeText(output, error);
            }
        });
    }

    /** Reads a result prefix, leaving the success body for the caller. */
    public static String readResult(DataInputStream input) throws IOException {
        return switch (input.readUnsignedByte()) {
            case 0 -> null;
            case 1 -> readText(input);
            default -> throw new ProtocolException("Invalid result flag");
        };
    }

    public static void requireEnd(DataInputStream input) throws IOException {
        if (input.read() != -1) {
            throw new ProtocolException("Unexpected transfer payload");
        }
    }

    public static boolean validName(String name) {
        if (name.isBlank() || name.startsWith(".") || name.endsWith(".") || name.endsWith(" ")
                || name.getBytes(StandardCharsets.UTF_8).length > 255
                || name.codePoints().anyMatch(c -> Character.isISOControl(c) || "<>:\"/\\|?*".indexOf(c) >= 0)) {
            return false;
        }
        String base = name.split("\\.", 2)[0].stripTrailing().toUpperCase(Locale.ROOT);
        return !base.matches("CON|PRN|AUX|NUL|CONIN\\$|CONOUT\\$|COM[1-9¹²³]|LPT[1-9¹²³]");
    }

    public static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException(exception);
        }
    }
}
