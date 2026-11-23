package com.dacs4.filesync.shared.protocol;

import com.dacs4.filesync.shared.dto.FileChangeNotification;
import com.dacs4.filesync.shared.dto.FileMetadata;
import com.dacs4.filesync.shared.dto.FileVersion;
import com.dacs4.filesync.shared.exception.ProtocolException;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.HexFormat;
import java.util.Objects;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;

/** Transfer envelope: request UUID, transfer UUID (zero for list), then the type-specific bytes. */
public final class FileTransferCodec {
    /** Optional suffix in the Phase 1 HELLO client name. */
    public static final String FILE_CHANGED_CAPABILITY = ";file-changed=1";
    public static final String NESTED_SYNC_CAPABILITY = ";file-changed=2";
    public static final String CONDITIONAL_MODIFY_CAPABILITY = ";conditional-modify=1";
    public static final String MODIFY_REPLACEMENT_CAPABILITY = ";modify-replace=1";
    public static final String RECONCILIATION_CAPABILITY = ";reconcile=1";
    public static final String VERSION_HISTORY_CAPABILITY = ";version-list=1";
    public static final String RESTORE_CAPABILITY = ";restore=1";
    public static final int RESULT_OK = 0;
    public static final int RESULT_ERROR = 1;
    public static final int RESULT_CONFLICT = 2;
    public static final int RESULT_NOT_IMPLEMENTED = 3;
    private static final int MODIFY_OPERATION = 1;
    public static final int MAX_PATH_COMPONENTS = 64;
    public static final UUID NONE = new UUID(0, 0);
    public static final int CHUNK_BYTES = 64 * 1024;
    public static final int HASH_BYTES = 32;
    public static final int MAX_LIST_FILES = 1000;
    // 100 * (16 + 2 + 512 + 32 + 8 + 8 + 8 + 4) plus headers fits the frame limit.
    public static final int MAX_LIST_VERSIONS = 100;
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
            || message.type() == MessageType.FILE_LIST_RESPONSE
            || message.type() == MessageType.VERSION_LIST_REQUEST
            || message.type() == MessageType.VERSION_LIST_RESPONSE
            || message.type() == MessageType.RESTORE_REQUEST || message.type() == MessageType.RESTORE_RESPONSE;
        if (request.equals(NONE) || (transfer.equals(NONE) && !list) || (list && !transfer.equals(NONE))) {
            throw new ProtocolException("Invalid request or transfer ID");
        }
        byte[] data = new byte[bytes.remaining()];
        bytes.get(data);
        return new Packet(message.type(), request, transfer, data);
    }

    /** Push messages have no request/transfer envelope. */
    public static ProtocolMessage encodeNotification(FileChangeNotification notification) throws IOException {
        if (!validRelativePath(notification.relativePath()) || NONE.equals(notification.eventId())
                || NONE.equals(notification.originSessionId())
                || notification.metadata().filter(file -> file.size() < 0).isPresent()) {
            throw new ProtocolException("Invalid file change notification");
        }
        byte[] payload = body(output -> {
            writeUuid(output, notification.eventId());
            output.writeByte(switch (notification.type()) {
                case CREATE -> 1;
                case MODIFY -> 2;
                case DELETE -> 3;
            });
            writeText(output, notification.relativePath());
            output.writeLong(notification.timestamp().toEpochMilli());
            writeUuid(output, notification.originSessionId());
            output.writeBoolean(notification.metadata().isPresent());
            if (notification.metadata().isPresent()) {
                writeMetadata(output, notification.metadata().get());
            }
            if (notification.sha256().isPresent()) {
                output.write(HexFormat.of().parseHex(notification.sha256().get()));
            }
            if (notification.previousSha256().isPresent()) {
                output.write(HexFormat.of().parseHex(notification.previousSha256().get()));
            }
        });
        return new ProtocolMessage(MessageType.FILE_CHANGED, payload);
    }

    public static FileChangeNotification decodeNotification(ProtocolMessage message) throws IOException {
        if (message.type() != MessageType.FILE_CHANGED || message.binaryPayload().length > 1200) {
            throw new ProtocolException("Invalid file change notification");
        }
        var input = new DataInputStream(new ByteArrayInputStream(message.binaryPayload()));
        try {
            UUID eventId = readUuid(input);
            var type = switch (input.readUnsignedByte()) {
                case 1 -> FileChangeNotification.Type.CREATE;
                case 2 -> FileChangeNotification.Type.MODIFY;
                case 3 -> FileChangeNotification.Type.DELETE;
                default -> throw new ProtocolException("Unknown file change type");
            };
            String path = readText(input);
            Instant timestamp = Instant.ofEpochMilli(input.readLong());
            UUID origin = readUuid(input);
            Optional<FileMetadata> metadata = switch (input.readUnsignedByte()) {
                case 0 -> Optional.empty();
                case 1 -> Optional.of(readMetadata(input));
                default -> throw new ProtocolException("Invalid notification metadata flag");
            };
            byte[] digest = input.readNBytes(2 * HASH_BYTES + 1);
            if (digest.length != 0 && digest.length != HASH_BYTES
                    && (type != FileChangeNotification.Type.MODIFY || digest.length != 2 * HASH_BYTES)) {
                throw new ProtocolException("Invalid notification digest");
            }
            if (!validRelativePath(path) || NONE.equals(eventId) || NONE.equals(origin)
                    || metadata.filter(file -> !path.equals(file.name())).isPresent()) {
                throw new ProtocolException("Invalid file change notification");
            }
            return new FileChangeNotification(eventId, type, path, timestamp, origin, metadata,
                digest.length == 0 ? Optional.empty() : Optional.of(HexFormat.of().formatHex(digest, 0, HASH_BYTES)),
                digest.length == 2 * HASH_BYTES
                    ? Optional.of(HexFormat.of().formatHex(digest, HASH_BYTES, 2 * HASH_BYTES)) : Optional.empty());
        } catch (EOFException | java.nio.charset.CharacterCodingException exception) {
            throw new ProtocolException("Malformed file change notification", exception);
        }
    }

    private static void writeUuid(DataOutputStream output, UUID id) throws IOException {
        output.writeLong(id.getMostSignificantBits());
        output.writeLong(id.getLeastSignificantBits());
    }

    private static UUID readUuid(DataInputStream input) throws IOException {
        return new UUID(input.readLong(), input.readLong());
    }

    public record RestoreRequest(String path, UUID versionId, byte[] expectedHash) {}

    public static byte[] restoreRequest(String path, UUID id, String expectedHash) throws IOException {
        if (!validRelativePath(path) || id == null || id.equals(NONE) || !validBaseHash(expectedHash)) {
            throw new ProtocolException("Invalid restore request");
        }
        return body(output -> {
            writeText(output, path);
            writeUuid(output, id);
            output.write(HexFormat.of().parseHex(expectedHash));
        });
    }

    public static RestoreRequest readRestoreRequest(DataInputStream input) throws IOException {
        String path = readText(input);
        UUID id = readUuid(input);
        byte[] expected = new byte[HASH_BYTES];
        input.readFully(expected);
        requireEnd(input);
        if (!validRelativePath(path) || id.equals(NONE)) {
            throw new ProtocolException("Invalid restore request");
        }
        return new RestoreRequest(path, id, expected);
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
        if (size < 0 || !validRelativePath(name)) {
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

    public static byte[] versionListResult(List<FileVersion> versions) throws IOException {
        if (versions.size() > MAX_LIST_VERSIONS) {
            throw new ProtocolException("Invalid version count");
        }
        return body(output -> {
            output.writeByte(RESULT_OK);
            output.writeInt(versions.size());
            for (FileVersion version : versions) {
                if (version.id().equals(NONE) || !validRelativePath(version.relativePath())
                        || !validBaseHash(version.sha256()) || version.size() < 0) {
                    throw new ProtocolException("Invalid version metadata");
                }
                writeUuid(output, version.id());
                writeText(output, version.relativePath());
                output.write(HexFormat.of().parseHex(version.sha256()));
                output.writeLong(version.size());
                output.writeLong(version.modifiedMillis());
                output.writeLong(version.archivedAt().getEpochSecond());
                output.writeInt(version.archivedAt().getNano());
            }
        });
    }

    /** Reads the success body after the result byte. */
    public static List<FileVersion> readVersionList(DataInputStream input, String requestedPath) throws IOException {
        int count = input.readInt();
        if (count < 0 || count > MAX_LIST_VERSIONS) {
            throw new ProtocolException("Invalid version count");
        }
        List<FileVersion> versions = new ArrayList<>(count);
        var ids = new java.util.HashSet<UUID>();
        for (int index = 0; index < count; index++) {
            UUID id = readUuid(input);
            String path = readText(input);
            byte[] hash = new byte[HASH_BYTES];
            input.readFully(hash);
            long size = input.readLong();
            long modified = input.readLong();
            long seconds = input.readLong();
            int nanos = input.readInt();
            if (id.equals(NONE) || !ids.add(id) || !validRelativePath(path) || !path.equals(requestedPath)
                    || size < 0 || nanos < 0 || nanos > 999_999_999
                    || seconds < Instant.MIN.getEpochSecond() || seconds > Instant.MAX.getEpochSecond()) {
                throw new ProtocolException("Invalid version metadata");
            }
            var version = new FileVersion(id, path, HexFormat.of().formatHex(hash), size, modified,
                Instant.ofEpochSecond(seconds, nanos));
            if (!versions.isEmpty() && FileVersion.NEWEST_FIRST.compare(versions.getLast(), version) > 0) {
                throw new ProtocolException("Invalid version ordering");
            }
            versions.add(version);
        }
        requireEnd(input);
        return List.copyOf(versions);
    }

    /** CREATE ends at size; a conditional MODIFY appends the operation and base digest. */
    public static byte[] modifyUploadInit(String path, long size, String baseHash) throws IOException {
        if (!validRelativePath(path) || size < 0 || !validBaseHash(baseHash)) {
            throw new ProtocolException("Invalid conditional MODIFY upload");
        }
        return body(output -> {
            writeText(output, path);
            output.writeLong(size);
            output.writeByte(MODIFY_OPERATION);
            writeText(output, baseHash);
        });
    }

    public static String readModifyBase(DataInputStream input) throws IOException {
        if (input.readUnsignedByte() != MODIFY_OPERATION) {
            throw new ProtocolException("Unknown upload operation");
        }
        String hash = readText(input);
        requireEnd(input);
        if (!validBaseHash(hash)) {
            throw new ProtocolException("Expected a 64-character hexadecimal SHA-256 base");
        }
        return hash;
    }

    private static boolean validBaseHash(String hash) {
        return hash != null && hash.matches("[0-9A-Fa-f]{64}");
    }

    public static byte[] modifyResult(int code, String message) throws IOException {
        if ((code != RESULT_CONFLICT && code != RESULT_NOT_IMPLEMENTED) || message == null) {
            throw new ProtocolException("Invalid conditional MODIFY result");
        }
        return body(output -> {
            output.writeByte(code);
            writeText(output, message);
        });
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

    public static boolean validRelativePath(String path) {
        if (path == null || path.isEmpty() || path.length() > MAX_TEXT_BYTES
                || !StandardCharsets.UTF_8.newEncoder().canEncode(path)
                || path.getBytes(StandardCharsets.UTF_8).length > MAX_TEXT_BYTES) {
            return false;
        }
        String[] components = path.split("/", -1);
        if (components.length > MAX_PATH_COMPONENTS) {
            return false;
        }
        for (String component : components) {
            if (!validName(component)) {
                return false;
            }
        }
        return true;
    }

    public static String relativePath(Path relative) throws ProtocolException {
        if (relative.isAbsolute() || relative.getRoot() != null) {
            throw new ProtocolException("Absolute transfer path");
        }
        var result = new java.util.StringJoiner("/");
        relative.forEach(component -> result.add(component.toString()));
        String path = result.toString();
        if (!validRelativePath(path)) {
            throw new ProtocolException("Invalid relative transfer path");
        }
        return path;
    }

    /**
     * Checked local resolution of protocol paths. The configured root and its ancestors
     * must be owned by the application user and protected from untrusted local writers.
     */
    public static final class SafeRoot {
        private static final LinkOption NOFOLLOW = LinkOption.NOFOLLOW_LINKS;

        private final Path path;
        private final BasicFileAttributes identity;

        public SafeRoot(Path configured) throws IOException {
            this(configured, true);
        }

        /** Opens an already configured root without creating any directory. */
        public static SafeRoot existing(Path configured) throws IOException {
            return new SafeRoot(configured, false);
        }

        private SafeRoot(Path configured, boolean create) throws IOException {
            Path absolute = configured.toAbsolutePath().normalize();
            Path parent = absolute.getRoot();
            directoryAttributes(parent);
            for (Path component : absolute) {
                parent = checkedDirectory(parent, parent.resolve(component), create);
            }
            identity = directoryAttributes(absolute);
            path = absolute;
        }

        public Path path() {
            return path;
        }

        public void check() throws IOException {
            BasicFileAttributes current = directoryAttributes(path);
            if (!Objects.equals(identity.fileKey(), current.fileKey())
                    || !identity.creationTime().equals(current.creationTime())) {
                throw new IOException("Sync/storage root was replaced: " + path);
            }
        }

        public Path resolve(String relative, boolean createParents) throws IOException {
            if (!validRelativePath(relative)) {
                throw new ProtocolException("Invalid relative transfer path");
            }
            check();
            String[] components = relative.split("/");
            Path parent = path;
            for (int index = 0; index < components.length - 1; index++) {
                parent = checkedDirectory(parent, parent.resolve(components[index]), createParents);
            }
            Path target = parent.resolve(components[components.length - 1]);
            try {
                regularAttributes(target);
            } catch (NoSuchFileException ignored) {
                // Missing targets are allowed. Callers that read must require a regular file.
            }
            return target;
        }

        private static Path checkedDirectory(Path parent, Path child, boolean create) throws IOException {
            try {
                directoryAttributes(child);
            } catch (NoSuchFileException exception) {
                if (!create) {
                    throw exception;
                }
                directoryAttributes(parent);
                try {
                    Files.createDirectory(child);
                } catch (FileAlreadyExistsException ignored) {
                    // Another transfer may have created the directory.
                }
                directoryAttributes(child);
            }
            return child;
        }

        public static BasicFileAttributes regularAttributes(Path file) throws IOException {
            BasicFileAttributes attributes = Files.readAttributes(file, BasicFileAttributes.class, NOFOLLOW);
            if (!attributes.isRegularFile() || attributes.isSymbolicLink() || attributes.isOther()
                    || !file.toRealPath().equals(file)) {
                throw new IOException("Expected a regular file without links: " + file);
            }
            return attributes;
        }

        public static BasicFileAttributes directoryAttributes(Path directory) throws IOException {
            BasicFileAttributes attributes = Files.readAttributes(directory, BasicFileAttributes.class, NOFOLLOW);
            if (!attributes.isDirectory() || attributes.isSymbolicLink() || attributes.isOther()
                    || !directory.toRealPath().equals(directory)) {
                throw new IOException("Expected a directory without links: " + directory);
            }
            return attributes;
        }

        /** Atomically publish an already verified file without replacing an existing name. */
        public void publish(Path temporary, String relative) throws IOException {
            Path target = resolve(relative, false);
            if (!path.equals(temporary.getParent())) {
                throw new IOException("Temporary file is not in the staging root");
            }
            regularAttributes(temporary);
            directoryAttributes(target.getParent());
            Files.createLink(target, temporary);
        }
    }

    public static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException(exception);
        }
    }
}
