package com.dacs4.filesync.server.service;

import com.dacs4.filesync.shared.dto.FileMetadata;
import com.dacs4.filesync.shared.dto.FileChangeNotification;
import com.dacs4.filesync.shared.dto.FileVersion;
import com.dacs4.filesync.shared.logging.AppLogging.WarningLimiter;
import java.util.Optional;
import java.time.Instant;
import com.dacs4.filesync.shared.exception.ProtocolException;
import com.dacs4.filesync.shared.protocol.MessageType;
import com.dacs4.filesync.shared.protocol.ProtocolMessage;

import java.io.*;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.BiConsumer;
import java.util.HexFormat;

import static com.dacs4.filesync.shared.protocol.FileTransferCodec.*;
import static com.dacs4.filesync.shared.protocol.MessageType.*;

/** One transfer at a time for this session. Only the session reader calls this service. */
public final class FileTransferService implements AutoCloseable {
    private static final System.Logger LOG = System.getLogger(FileTransferService.class.getName());
    private static final System.Logger RESTORE_LOG = System.getLogger("com.dacs4.filesync.log.Restore");
    private static final WarningLimiter STALE_BASE_LOG = new WarningLimiter();
    private static final long IDLE_NANOS = TimeUnit.SECONDS.toNanos(30);
    private static final byte[] EMPTY = new byte[0];

    private final FileStorageService storage;
    private final BiConsumer<Path, String> uploadCommitted;
    private final boolean nested;
    private final boolean conditionalModify;
    private final boolean modifyReplacement;
    private final boolean reconciliation;
    private final boolean versionHistory;
    private final Consumer<FileChangeNotification> restoreCommitted;
    private final Consumer<FileChangeNotification> modifyCommitted;
    private final UUID origin;
    private byte[] expectedBase;
    private UUID transfer;
    private long lastActivity;
    private long remaining;
    private boolean downloadEnded;
    private MessageDigest digest;
    private Path temporary;
    private String target;
    private OutputStream upload;
    private InputStream download;
    private String downloadLogPath;

    public FileTransferService(FileStorageService storage, Consumer<Path> uploadCommitted) {
        this(storage, true, (path, hash) -> uploadCommitted.accept(path));
    }

    public FileTransferService(FileStorageService storage, boolean nested, BiConsumer<Path, String> uploadCommitted) {
        this(storage, nested, false, uploadCommitted);
    }

    public FileTransferService(FileStorageService storage, boolean nested, boolean conditionalModify,
                               BiConsumer<Path, String> uploadCommitted) {
        this(storage, nested, conditionalModify, NONE, uploadCommitted, null);
    }

    public FileTransferService(FileStorageService storage, boolean nested, boolean conditionalModify,
                               UUID origin, BiConsumer<Path, String> uploadCommitted,
                               Consumer<FileChangeNotification> modifyCommitted) {
        this(storage, nested, conditionalModify, origin, uploadCommitted, modifyCommitted, false);
    }

    public FileTransferService(FileStorageService storage, boolean nested, boolean conditionalModify,
                               UUID origin, BiConsumer<Path, String> uploadCommitted,
                               Consumer<FileChangeNotification> modifyCommitted, boolean reconciliation) {
        this(storage, nested, conditionalModify, origin, uploadCommitted, modifyCommitted, reconciliation, false);
    }

    public FileTransferService(FileStorageService storage, boolean nested, boolean conditionalModify,
                               UUID origin, BiConsumer<Path, String> uploadCommitted,
                               Consumer<FileChangeNotification> modifyCommitted, boolean reconciliation,
                               boolean versionHistory) {
        this(storage, nested, conditionalModify, origin, uploadCommitted, modifyCommitted, reconciliation,
            versionHistory, null);
    }

    public FileTransferService(FileStorageService storage, boolean nested, boolean conditionalModify,
                               UUID origin, BiConsumer<Path, String> uploadCommitted,
                               Consumer<FileChangeNotification> modifyCommitted, boolean reconciliation,
                               boolean versionHistory, Consumer<FileChangeNotification> restoreCommitted) {
        this.storage = storage;
        this.nested = nested;
        this.conditionalModify = conditionalModify;
        this.origin = origin;
        this.modifyReplacement = modifyCommitted != null;
        this.modifyCommitted = modifyCommitted;
        this.uploadCommitted = uploadCommitted;
        this.reconciliation = reconciliation;
        this.versionHistory = versionHistory;
        this.restoreCommitted = restoreCommitted;
    }

    private boolean validTarget(String path) {
        return nested ? validRelativePath(path) : validName(path);
    }

    public ProtocolMessage handle(ProtocolMessage message) throws IOException {
        Packet packet = decode(message);
        expireIfIdle();
        try {
            return switch (packet.type()) {
                case UPLOAD_INIT -> beginUpload(packet);
                case FILE_CHUNK -> chunk(packet);
                case UPLOAD_END -> endUpload(packet);
                case DOWNLOAD_REQUEST -> beginDownload(packet);
                case DOWNLOAD_RESULT -> endDownload(packet);
                case FILE_LIST_REQUEST -> list(packet);
                case VERSION_LIST_REQUEST -> listVersions(packet);
                case RESTORE_REQUEST -> restore(packet);
                default -> throw new ProtocolException("Unexpected client transfer message");
            };
        } catch (ProtocolException exception) {
            throw exception;
        } catch (EOFException | java.nio.charset.CharacterCodingException exception) {
            throw new ProtocolException("Malformed transfer payload", exception);
        } catch (IOException exception) {
            String transferPath = packet.transferId().equals(transfer) ? (target != null ? target : downloadLogPath) : null;
            String failedPath = transferPath == null ? "" : ": " + transferPath;
            if (packet.transferId().equals(transfer)) {
                close();
            }
            MessageType responseType = switch (packet.type()) {
                case FILE_LIST_REQUEST -> FILE_LIST_RESPONSE;
                case VERSION_LIST_REQUEST -> VERSION_LIST_RESPONSE;
                case RESTORE_REQUEST -> RESTORE_RESPONSE;
                case DOWNLOAD_REQUEST, DOWNLOAD_RESULT -> DOWNLOAD_RESULT;
                case FILE_CHUNK -> packet.data().length == 0 ? DOWNLOAD_RESULT : UPLOAD_RESULT;
                default -> UPLOAD_RESULT;
            };
            if (reconciliation && exception instanceof FileAlreadyExistsException && responseType == UPLOAD_RESULT) {
                LOG.log(System.Logger.Level.DEBUG, "Upload rejected because destination exists" + failedPath, exception);
                return reply(packet, responseType, modifyResult(RESULT_CONFLICT, "Tệp cùng tên đã tồn tại trên máy chủ."));
            }
            if (responseType != RESTORE_RESPONSE) {
                String operation = switch (responseType) {
                    case UPLOAD_RESULT -> "Upload";
                    case DOWNLOAD_RESULT -> "Download";
                    case VERSION_LIST_RESPONSE -> "Version history";
                    default -> "File listing";
                };
                boolean recoverable = exception instanceof TransferFailure || exception instanceof NoSuchFileException
                    || exception instanceof FileAlreadyExistsException || exception instanceof VersionStore.HistoryTimeout;
                LOG.log(recoverable ? System.Logger.Level.WARNING : System.Logger.Level.ERROR,
                    operation + " failed" + failedPath, exception);
            }
            String error = switch (exception) {
                case FileAlreadyExistsException ignored -> "Tệp cùng tên đã tồn tại trên máy chủ.";
                case NoSuchFileException ignored -> "Không tìm thấy tệp trên máy chủ.";
                case VersionStore.HistoryTimeout ignored -> "Đọc lịch sử phiên bản quá thời gian cho phép.";
                case TransferFailure failure -> failure.getMessage();
                default -> "Máy chủ không thể đọc hoặc lưu tệp.";
            };
            return reply(packet, responseType, result(error));
        }
    }

    private ProtocolMessage beginUpload(Packet packet) throws IOException {
        var input = packet.body();
        String name = readText(input);
        long size = input.readLong();
        if (input.available() != 0) {
            if (!conditionalModify) {
                return reply(packet, UPLOAD_RESULT, modifyResult(RESULT_NOT_IMPLEMENTED, "Chưa thương lượng giao thức MODIFY."));
            }
            String baseHash = readModifyBase(input);
            if (!validRelativePath(name)) {
                throw new ProtocolException("Invalid conditional MODIFY path");
            }
            if (size < 0 || size > storage.maxFileBytes()) {
                LOG.log(System.Logger.Level.WARNING, "Upload rejected; invalid size: {0}", name);
                return reply(packet, UPLOAD_RESULT, result("Dung lượng tệp không hợp lệ."));
            }
            byte[] base = HexFormat.of().parseHex(baseHash);
            if (!storage.matchesBase(name, base)) {
                return baseConflict(packet, name);
            }
            if (!modifyReplacement) {
                return reply(packet, UPLOAD_RESULT,
                    modifyResult(RESULT_NOT_IMPLEMENTED, "Chưa thương lượng thay thế tệp bằng MODIFY."));
            }
            begin(packet, size);
            target = name;
            expectedBase = base;
            return openUpload(packet);
        }
        if (!validTarget(name) || size < 0 || size > storage.maxFileBytes()) {
            throw new TransferFailure("Tên hoặc dung lượng tệp không hợp lệ. Giới hạn: " + storage.maxFileBytes() + " byte.");
        }
        begin(packet, size);
        target = name;
        try {
            Path destination = storage.file(name, false);
            if (Files.exists(destination, LinkOption.NOFOLLOW_LINKS)) {
                throw new FileAlreadyExistsException(name);
            }
        } catch (NoSuchFileException ignored) {
            // Missing parents are created only after the upload has been verified.
        }
        return openUpload(packet);
    }

    private ProtocolMessage openUpload(Packet packet) throws IOException {
        temporary = storage.temporaryUpload();
        upload = Files.newOutputStream(temporary, LinkOption.NOFOLLOW_LINKS);
        return reply(packet, UPLOAD_READY, EMPTY);
    }

    private static ProtocolMessage baseConflict(Packet packet, String path) throws IOException {
        LOG.log(STALE_BASE_LOG.allow() ? System.Logger.Level.WARNING : System.Logger.Level.DEBUG,
            "MODIFY conflict; server baseline differs: {0}", path);
        return reply(packet, UPLOAD_RESULT,
            modifyResult(RESULT_CONFLICT, "Phiên bản gốc không tồn tại hoặc mã SHA-256 không khớp."));
    }

    private ProtocolMessage beginDownload(Packet packet) throws IOException {
        var input = packet.body();
        String name = readText(input);
        requireEnd(input);
        if (!validTarget(name)) {
            throw new TransferFailure("Tên tệp không hợp lệ.");
        }
        Path path = storage.file(name, false);
        FileMetadata metadata = storage.metadata(path);
        if (metadata.size() > storage.maxFileBytes()) {
            throw new TransferFailure("Tệp vượt giới hạn tải xuống.");
        }
        begin(packet, metadata.size());
        downloadLogPath = name;
        download = Files.newInputStream(path, LinkOption.NOFOLLOW_LINKS);
        return reply(packet, DOWNLOAD_INFO, body(output -> writeMetadata(output, metadata)));
    }

    private void begin(Packet packet, long size) throws IOException {
        if (transfer != null) {
            if (transfer.equals(packet.transferId())) {
                throw new ProtocolException("Duplicate transfer initialization");
            }
            throw new TransferFailure("Phiên này đang truyền một tệp khác.");
        }
        transfer = packet.transferId();
        remaining = size;
        digest = sha256();
        lastActivity = System.nanoTime();
    }

    private ProtocolMessage chunk(Packet packet) throws IOException {
        requireTransfer(packet);
        byte[] data = packet.data();
        if (upload != null) {
            if (data.length == 0 || data.length > CHUNK_BYTES || data.length > remaining) {
                throw new ProtocolException("Invalid upload chunk size");
            }
            upload.write(data);
            digest.update(data);
            remaining -= data.length;
            return reply(packet, UPLOAD_READY, EMPTY);
        }
        if (download == null || data.length != 0 || downloadEnded) {
            throw new ProtocolException("Unexpected download pull");
        }
        if (remaining == 0) {
            if (download.read() != -1) {
                throw new TransferFailure("Tệp trên máy chủ đã thay đổi trong khi tải xuống.");
            }
            downloadEnded = true;
            return reply(packet, DOWNLOAD_END, digest.digest());
        }
        int expected = (int) Math.min(CHUNK_BYTES, remaining);
        byte[] bytes = download.readNBytes(expected);
        if (bytes.length != expected) {
            throw new TransferFailure("Tệp trên máy chủ đã thay đổi trong khi tải xuống.");
        }
        digest.update(bytes);
        remaining -= bytes.length;
        return reply(packet, FILE_CHUNK, bytes);
    }

    private ProtocolMessage endUpload(Packet packet) throws IOException {
        requireTransfer(packet);
        byte[] hash = packet.data();
        if (upload == null || (hash.length != 0 && hash.length != HASH_BYTES)) {
            throw new ProtocolException("Invalid upload end");
        }
        if (hash.length != 0) {
            if (remaining != 0 || !MessageDigest.isEqual(hash, digest.digest())) {
                throw new TransferFailure("Dung lượng hoặc mã SHA-256 của tệp tải lên không khớp.");
            }
            upload.close();
            upload = null;
            if (expectedBase == null) {
                Path committed = storage.commit(temporary, target);
                temporary = null;
                uploadCommitted.accept(committed, HexFormat.of().formatHex(hash));
            } else {
                if (MessageDigest.isEqual(expectedBase, hash)) {
                    throw new TransferFailure("Nội dung MODIFY trùng với phiên bản gốc.");
                }
                if (storage.replace(temporary, target, expectedBase) == null) {
                    String conflictingPath = target;
                    close();
                    return baseConflict(packet, conflictingPath);
                }
                temporary = null;
                modifyCommitted.accept(modification(target, HexFormat.of().formatHex(hash), expectedBase));
            }
        }
        close();
        return reply(packet, UPLOAD_RESULT, result(null));
    }

    private ProtocolMessage endDownload(Packet packet) throws IOException {
        requireTransfer(packet);
        byte[] data = packet.data();
        if (download == null || data.length != 1 || (data[0] != 0 && data[0] != 1)
                || (data[0] == 0 && !downloadEnded)) {
            throw new ProtocolException("Invalid download result");
        }
        close();
        return reply(packet, DOWNLOAD_RESULT, result(null));
    }

    private ProtocolMessage list(Packet packet) throws IOException {
        if (packet.data().length != 0) {
            if (!reconciliation) {
                throw new ProtocolException("Invalid list request");
            }
            var input = packet.body();
            String path = readText(input);
            requireEnd(input);
            if (!validTarget(path)) {
                throw new ProtocolException("Invalid reconciliation path");
            }
            var version = storage.version(path);
            return reply(packet, FILE_LIST_RESPONSE, body(output -> {
                output.writeByte(RESULT_OK);
                output.writeBoolean(version != null);
                if (version != null) {
                    writeMetadata(output, version.metadata());
                    output.write(version.hash());
                }
            }));
        }
        var files = storage.list(nested);
        return reply(packet, FILE_LIST_RESPONSE, body(output -> {
            output.writeByte(0);
            output.writeInt(files.size());
            for (var file : files) {
                writeMetadata(output, file);
            }
        }));
    }

    private ProtocolMessage listVersions(Packet packet) throws IOException {
        if (!versionHistory) {
            return reply(packet, VERSION_LIST_RESPONSE,
                modifyResult(RESULT_NOT_IMPLEMENTED, "Chưa thương lượng giao thức lịch sử phiên bản."));
        }
        var input = packet.body();
        String path = readText(input);
        requireEnd(input);
        if (!validTarget(path)) {
            return reply(packet, VERSION_LIST_RESPONSE, result("Đường dẫn tệp không hợp lệ."));
        }
        return reply(packet, VERSION_LIST_RESPONSE, versionListResult(storage.listVersions(path)));
    }

    private ProtocolMessage restore(Packet packet) throws IOException {
        if (restoreCommitted == null) {
            RESTORE_LOG.log(System.Logger.Level.DEBUG, "Restore rejected; capability unavailable");
            return reply(packet, RESTORE_RESPONSE, modifyResult(RESULT_NOT_IMPLEMENTED, "Máy chủ không hỗ trợ khôi phục."));
        }
        if (transfer != null) {
            RESTORE_LOG.log(System.Logger.Level.WARNING, "Restore rejected; file transfer already active");
            return reply(packet, RESTORE_RESPONSE, result("Phiên đang truyền tệp."));
        }
        var request = readRestoreRequest(packet.body());
        if (!validTarget(request.path())) {
            return reply(packet, RESTORE_RESPONSE, result("Đường dẫn tệp không hợp lệ."));
        }
        FileVersion selected;
        try {
            selected = storage.restore(request.path(), request.versionId(), request.expectedHash());
        } catch (IOException exception) {
            RESTORE_LOG.log(System.Logger.Level.ERROR, "Server restore failed: " + request.path(), exception);
            throw exception;
        }
        if (selected == null) {
            RESTORE_LOG.log(System.Logger.Level.WARNING, "Server restore rejected; current version changed: {0}", request.path());
            return reply(packet, RESTORE_RESPONSE,
                modifyResult(RESULT_CONFLICT, "Tệp hiện tại đã thay đổi hoặc không còn tồn tại."));
        }
        restoreCommitted.accept(modification(request.path(), selected.sha256(), request.expectedHash()));
        RESTORE_LOG.log(System.Logger.Level.INFO, "Server restored: {0}", request.path());
        return reply(packet, RESTORE_RESPONSE, result(null));
    }

    private FileChangeNotification modification(String path, String sha256, byte[] previous) {
        return new FileChangeNotification(UUID.randomUUID(), FileChangeNotification.Type.MODIFY, path, Instant.now(),
            origin, Optional.empty(), Optional.of(sha256), Optional.of(HexFormat.of().formatHex(previous)));
    }

    private void requireTransfer(Packet packet) throws IOException {
        if (!packet.transferId().equals(transfer)) {
            throw new TransferFailure("Phiên truyền tệp đã hết hạn hoặc không tồn tại.");
        }
        lastActivity = System.nanoTime();
    }

    public void expireIfIdle() {
        if (transfer != null && System.nanoTime() - lastActivity >= IDLE_NANOS) {
            close();
        }
    }

    private static ProtocolMessage reply(Packet packet, MessageType type, byte[] data) {
        return encode(type, packet.requestId(), packet.transferId(), data);
    }

    @Override
    public void close() {
        try (InputStream input = download; OutputStream output = upload) {
            // Resource closure precedes deletion on Windows.
        } catch (IOException exception) {
            LOG.log(System.Logger.Level.WARNING, "Could not close transfer stream", exception);
        } finally {
            if (temporary != null) {
                try {
                    storage.discard(temporary);
                } catch (IOException exception) {
                    LOG.log(System.Logger.Level.WARNING, "Could not delete incomplete upload", exception);
                }
            }
            temporary = null;
            expectedBase = null;
            upload = null;
            download = null;
            downloadLogPath = null;
            transfer = null;
            downloadEnded = false;
        }
    }

    private static final class TransferFailure extends IOException {
        TransferFailure(String message) {
            super(message);
        }
    }
}
