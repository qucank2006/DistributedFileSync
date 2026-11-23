package com.dacs4.filesync.network;

import com.dacs4.filesync.shared.dto.FileMetadata;
import com.dacs4.filesync.shared.dto.FileVersion;
import com.dacs4.filesync.shared.exception.ProtocolException;
import com.dacs4.filesync.shared.protocol.FileTransferCodec.Packet;
import com.dacs4.filesync.shared.protocol.MessageType;
import com.dacs4.filesync.sync.SyncFileRegistry;

import java.io.*;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.List;
import java.util.HexFormat;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import static com.dacs4.filesync.shared.protocol.FileTransferCodec.*;
import static com.dacs4.filesync.shared.protocol.MessageType.*;

public final class FileTransferClient {
    private static final System.Logger LOG = System.getLogger(FileTransferClient.class.getName());
    private static final byte[] EMPTY = new byte[0];
    private final TcpClient connection;

    public FileTransferClient(TcpClient connection) {
        this.connection = connection;
    }

    @FunctionalInterface
    public interface Progress {
        void update(long bytes, long total);
    }

    @FunctionalInterface
    public interface Publication {
        void run() throws IOException;
    }

    @FunctionalInterface
    public interface PublicationGuard {
        void publish(Publication publication) throws IOException;
    }

    public static class Failure extends IOException {
        private final int resultCode;

        public Failure(String message) {
            this(RESULT_ERROR, message);
        }

        public Failure(int resultCode, String message) {
            super(message);
            this.resultCode = resultCode;
        }

        public int resultCode() {
            return resultCode;
        }
    }

    public static final class LocalReplacementConflict extends Failure {
        private LocalReplacementConflict(String message) {
            super(RESULT_CONFLICT, message);
        }
    }

    public static final class Cancelled extends IOException {}

    public List<FileMetadata> list() throws IOException {
        Packet reply = request(FILE_LIST_REQUEST, NONE, EMPTY);
        requireType(reply, FILE_LIST_RESPONSE);
        var input = reply.body();
        checkResult(input);
        return readList(input.readAllBytes());
    }

    /** Blocking protocol call for use off the JavaFX thread under the file-operation lock. */
    public List<FileVersion> listVersions(String path) throws IOException {
        if (!connection.isOpen() || connection.debugSessionId() == null) {
            throw new IOException("Phiên kết nối không còn hoạt động.");
        }
        if (!connection.supportsVersionHistory()) {
            throw new Failure(RESULT_NOT_IMPLEMENTED, "Máy chủ không hỗ trợ lịch sử phiên bản.");
        }
        if (!validRelativePath(path)) {
            throw new ProtocolException("Invalid version history path");
        }
        Packet reply = request(VERSION_LIST_REQUEST, NONE, body(output -> writeText(output, path)));
        requireType(reply, VERSION_LIST_RESPONSE);
        var input = reply.body();
        checkResult(input);
        return readVersionList(input, path);
    }

    /** Blocking protocol call for use off the JavaFX thread under the file-operation lock. */
    public void restoreVersion(String path, UUID versionId, String expectedHash) throws IOException {
        if (!connection.isOpen() || connection.debugSessionId() == null) {
            throw new IOException("Phiên kết nối không còn hoạt động.");
        }
        if (!connection.supportsRestore()) {
            throw new Failure(RESULT_NOT_IMPLEMENTED, "Máy chủ không hỗ trợ khôi phục.");
        }
        Packet reply = request(RESTORE_REQUEST, NONE, restoreRequest(path, versionId, expectedHash));
        requireType(reply, RESTORE_RESPONSE);
        checkOnlyResult(reply);
    }

    public record ServerVersion(FileMetadata metadata, String sha256) {}

    public Optional<ServerVersion> serverVersion(String path) throws IOException {
        if (!connection.supportsReconciliation()) {
            throw new Failure(RESULT_NOT_IMPLEMENTED, "Máy chủ không hỗ trợ đối chiếu tệp.");
        }
        Packet reply = request(FILE_LIST_REQUEST, NONE, body(output -> writeText(output, path)));
        requireType(reply, FILE_LIST_RESPONSE);
        var input = reply.body();
        checkResult(input);
        int present = input.readUnsignedByte();
        if (present == 0) {
            requireEnd(input);
            return Optional.empty();
        }
        if (present != 1) {
            throw new ProtocolException("Invalid server version flag");
        }
        var metadata = readMetadata(input);
        byte[] hash = input.readNBytes(HASH_BYTES);
        requireEnd(input);
        if (!path.equals(metadata.relativePath()) || hash.length != HASH_BYTES) {
            throw new ProtocolException("Invalid server version");
        }
        return Optional.of(new ServerVersion(metadata, HexFormat.of().formatHex(hash)));
    }

    /**
     * Checks MODIFY readiness and cancels any opened staging transfer without sending file bytes.
     * Run off the JavaFX thread while holding the connection's file-operation lock.
     */
    public void requestModifyUpload(String path, long size, String expectedBaseHash) throws IOException {
        if (!connection.supportsConditionalModify()) {
            throw new Failure(RESULT_NOT_IMPLEMENTED, "Máy chủ không công bố hỗ trợ giao thức MODIFY có điều kiện.");
        }
        UUID transfer = UUID.randomUUID();
        checkReady(request(UPLOAD_INIT, transfer, modifyUploadInit(path, size, expectedBaseHash)));
        Packet cancelled = request(UPLOAD_END, transfer, EMPTY);
        requireType(cancelled, UPLOAD_RESULT);
        checkOnlyResult(cancelled);
    }

    /** Explicit MODIFY only; callers must hold the connection file-operation lock. */
    public void uploadModified(Path source, String relative, String expectedBaseHash,
                               BooleanSupplier cancelled, Progress progress) throws IOException {
        uploadModified(source, relative, expectedBaseHash, Optional.empty(), cancelled, progress);
    }

    public void uploadModified(Path source, String relative, String expectedBaseHash,
                               Optional<String> expectedUploadHash, BooleanSupplier cancelled,
                               Progress progress) throws IOException {
        if (!connection.supportsModifyReplacement()) {
            throw new Failure(RESULT_NOT_IMPLEMENTED, "Máy chủ không hỗ trợ thay thế tệp bằng MODIFY.");
        }
        upload(source, relative, expectedUploadHash, java.util.Objects.requireNonNull(expectedBaseHash),
            cancelled, progress);
    }

    public void upload(Path source, BooleanSupplier cancelled, Progress progress) throws IOException {
        upload(source, source.getFileName().toString(), cancelled, progress);
    }

    public void upload(Path source, String relative, BooleanSupplier cancelled, Progress progress) throws IOException {
        upload(source, relative, Optional.empty(), cancelled, progress);
    }

    public void upload(Path source, String relative, Optional<String> expectedHash,
                       BooleanSupplier cancelled, Progress progress) throws IOException {
        upload(source, relative, expectedHash, null, cancelled, progress);
    }

    private void upload(Path source, String relative, Optional<String> expectedHash, String baseHash,
                        BooleanSupplier cancelled, Progress progress) throws IOException {
        if (!validRelativePath(relative)) {
            throw new Failure("Tên tệp không hợp lệ trên máy chủ.");
        }
        Path inputPath = source.toAbsolutePath().normalize();
        long total = SafeRoot.regularAttributes(inputPath).size();
        UUID transfer = UUID.randomUUID();
        boolean opened = false;
        try (InputStream input = Files.newInputStream(inputPath, LinkOption.NOFOLLOW_LINKS)) {
            checkCancelled(cancelled);
            byte[] init = baseHash == null ? body(output -> {
                writeText(output, relative);
                output.writeLong(total);
            }) : modifyUploadInit(relative, total, baseHash);
            Packet ready = request(UPLOAD_INIT, transfer, init);
            checkReady(ready);
            opened = true;
            MessageDigest digest = sha256();
            progress.update(0, total);
            long sent = 0;
            while (sent < total) {
                checkCancelled(cancelled);
                int expected = (int) Math.min(CHUNK_BYTES, total - sent);
                byte[] bytes = input.readNBytes(expected);
                if (bytes.length != expected) {
                    throw new Failure("Tệp nguồn đã thay đổi kích thước trong khi tải lên.");
                }
                digest.update(bytes);
                checkReady(request(FILE_CHUNK, transfer, bytes));
                sent += bytes.length;
                progress.update(sent, total);
            }
            if (input.read() != -1) {
                throw new Failure("Tệp nguồn đã thay đổi kích thước trong khi tải lên.");
            }
            checkCancelled(cancelled);
            byte[] hash = digest.digest();
            if (expectedHash.isPresent() && !expectedHash.get().equals(HexFormat.of().formatHex(hash))) {
                throw new Failure("Tệp nguồn đã thay đổi trong khi tải lên.");
            }
            Packet result = request(UPLOAD_END, transfer, hash);
            requireType(result, UPLOAD_RESULT);
            opened = false;
            checkOnlyResult(result);
        } finally {
            if (opened) {
                abort(UPLOAD_END, transfer, EMPTY);
            }
        }
    }

    public Path download(FileMetadata selected, Path directory, BooleanSupplier cancelled,
                         Progress progress) throws IOException {
        return downloadToSyncRoot(selected, directory, Optional.empty(), cancelled, progress);
    }

    public Path downloadToSyncRoot(FileMetadata selected, Path directory, Optional<String> expectedHash,
                                   BooleanSupplier cancelled, Progress progress) throws IOException {
        checkCancelled(cancelled);
        return downloadToRoot(selected, new SafeRoot(directory), expectedHash, cancelled, progress);
    }

    public Path downloadToRoot(FileMetadata selected, SafeRoot root, Optional<String> expectedHash,
                               BooleanSupplier cancelled, Progress progress) throws IOException {
        return downloadToRoot(selected, root, expectedHash, cancelled, progress, Publication::run);
    }

    public Path downloadToRoot(FileMetadata selected, SafeRoot root, Optional<String> expectedHash,
                               BooleanSupplier cancelled, Progress progress, PublicationGuard guard) throws IOException {
        return downloadToRoot(selected, root, expectedHash, null, false, cancelled, progress, guard);
    }

    public Path downloadReconciled(ServerVersion version, SafeRoot root, SyncFileRegistry.Fingerprint localBase,
                                   BooleanSupplier cancelled, Progress progress, PublicationGuard guard) throws IOException {
        return downloadToRoot(version.metadata(), root, Optional.of(version.sha256()),
            localBase, true, cancelled, progress, guard);
    }

    /** Downloads verified server bytes into an empty, private preview root. */
    public Path downloadSnapshot(ServerVersion version, SafeRoot root, BooleanSupplier cancelled,
                                 Progress progress) throws IOException {
        return downloadToRoot(version.metadata(), root, Optional.of(version.sha256()),
            null, true, cancelled, progress, Publication::run);
    }

    public Path downloadModified(FileMetadata selected, SafeRoot root, String expectedHash,
                                 SyncFileRegistry.Fingerprint localBase, boolean verifyMetadata,
                                 BooleanSupplier cancelled, Progress progress) throws IOException {
        return downloadModified(selected, root, expectedHash, localBase, verifyMetadata, cancelled, progress, Publication::run);
    }

    public Path downloadModified(FileMetadata selected, SafeRoot root, String expectedHash,
                                 SyncFileRegistry.Fingerprint localBase, boolean verifyMetadata,
                                 BooleanSupplier cancelled, Progress progress, PublicationGuard guard) throws IOException {
        return downloadToRoot(selected, root, Optional.of(expectedHash),
            java.util.Objects.requireNonNull(localBase), verifyMetadata, cancelled, progress, guard);
    }

    private Path downloadToRoot(FileMetadata selected, SafeRoot root, Optional<String> expectedHash,
                                SyncFileRegistry.Fingerprint localBase, boolean verifyMetadata,
                                BooleanSupplier cancelled, Progress progress, PublicationGuard guard) throws IOException {
        String relative = selected.relativePath();
        checkCancelled(cancelled);
        Path target = root.resolve(relative, localBase == null);
        if (localBase == null && Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
            throw new FileAlreadyExistsException(target.toString());
        }
        UUID transfer = UUID.randomUUID();
        Path temporary = null;
        boolean opened = false;
        try {
            Packet info = request(DOWNLOAD_REQUEST, transfer,
                body(output -> writeText(output, relative)));
            requireType(info, DOWNLOAD_INFO);
            opened = true;
            var metadata = info.body();
            FileMetadata actual = readMetadata(metadata);
            requireEnd(metadata);
            if (!selected.name().equals(actual.name())) {
                throw new ProtocolException("Unexpected download filename");
            }
            if (verifyMetadata && (selected.size() != actual.size()
                    || selected.modifiedMillis() != actual.modifiedMillis())) {
                throw new Failure(RESULT_CONFLICT, "Thông tin tệp máy chủ đã thay đổi.");
            }

            root.check();
            temporary = Files.createTempFile(root.path(), ".download-", ".part");
            receive(transfer, actual.size(), temporary, expectedHash, cancelled, progress);
            checkCancelled(cancelled);
            Packet result = request(DOWNLOAD_RESULT, transfer, new byte[]{0});
            requireType(result, DOWNLOAD_RESULT);
            opened = false;
            checkOnlyResult(result);
            checkCancelled(cancelled);
            Path staged = temporary;
            if (localBase == null) {
                guard.publish(() -> {
                    checkCancelled(cancelled);
                    root.publish(staged, relative);
                });
            } else {
                SafeRoot.regularAttributes(temporary);
                checkLocalReplacement(root, relative, localBase, cancelled);
                guard.publish(() -> {
                    checkCancelled(cancelled);
                    Path destination = checkLocalAttributes(root, relative, localBase);
                    Files.move(staged, destination, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                });
            }
            return target;
        } finally {
            try {
                if (temporary != null) {
                    try {
                        root.check();
                        Files.deleteIfExists(temporary);
                    } catch (IOException exception) {
                        LOG.log(System.Logger.Level.WARNING,
                            "Could not remove download staging file", exception);
                    }
                }
            } finally {
                if (opened) {
                    abort(DOWNLOAD_RESULT, transfer, new byte[]{1});
                }
            }
        }
    }

    private void checkLocalReplacement(SafeRoot root, String relative,
                                       SyncFileRegistry.Fingerprint localBase, BooleanSupplier cancelled)
            throws IOException {
        try {
            Path target = root.resolve(relative, false);
            if (!localBase.equals(SyncFileRegistry.read(target, cancelled))) {
                throw new LocalReplacementConflict("Tệp cục bộ đã thay đổi trong khi tải xuống.");
            }
        } catch (NoSuchFileException | SyncFileRegistry.FileChangedException exception) {
            throw new LocalReplacementConflict("Tệp cục bộ đã thay đổi hoặc không còn tồn tại.");
        }
    }

    private Path checkLocalAttributes(SafeRoot root, String relative, SyncFileRegistry.Fingerprint localBase)
            throws IOException {
        try {
            Path target = root.resolve(relative, false);
            var attributes = SafeRoot.regularAttributes(target);
            if (attributes.size() != localBase.size() || !attributes.lastModifiedTime().equals(localBase.modified())
                    || !attributes.creationTime().equals(localBase.created())
                    || !java.util.Objects.equals(attributes.fileKey(), localBase.fileKey())) {
                throw new LocalReplacementConflict("Tệp cục bộ đã thay đổi trước khi thay thế.");
            }
            return target;
        } catch (NoSuchFileException exception) {
            throw new LocalReplacementConflict("Tệp cục bộ đã thay đổi hoặc không còn tồn tại.");
        }
    }

    private void receive(UUID transfer, long total, Path temporary, Optional<String> expectedHash,
                         BooleanSupplier cancelled, Progress progress) throws IOException {
        MessageDigest digest = sha256();
        long received = 0;
        progress.update(0, total);
        try (OutputStream output = Files.newOutputStream(temporary, LinkOption.NOFOLLOW_LINKS)) {
            while (received < total) {
                checkCancelled(cancelled);
                Packet packet = request(FILE_CHUNK, transfer, EMPTY);
                requireType(packet, FILE_CHUNK);
                byte[] bytes = packet.data();
                if (bytes.length != Math.min(CHUNK_BYTES, total - received)) {
                    throw new ProtocolException("Invalid download chunk length");
                }
                output.write(bytes);
                digest.update(bytes);
                received += bytes.length;
                progress.update(received, total);
            }
            checkCancelled(cancelled);
            Packet end = request(FILE_CHUNK, transfer, EMPTY);
            requireType(end, DOWNLOAD_END);
            byte[] hash = digest.digest();
            if (!MessageDigest.isEqual(end.data(), hash)) {
                throw new Failure("Mã SHA-256 không khớp. Tệp tải xuống chưa được lưu.");
            }
            if (expectedHash.isPresent() && !expectedHash.get().equals(HexFormat.of().formatHex(hash))) {
                throw new Failure(RESULT_CONFLICT, "Phiên bản máy chủ không khớp. Tệp tải xuống chưa được lưu.");
            }
        }
    }

    private Packet request(MessageType type, UUID transfer, byte[] data) throws IOException {
        return connection.exchange(type, transfer, data);
    }

    private void abort(MessageType type, UUID transfer, byte[] data) {
        try {
            request(type, transfer, data);
        } catch (IOException exception) {
            // A failed exchange closes the socket and the server discards any partial transfer.
            LOG.log(System.Logger.Level.DEBUG, "Could not send transfer cancellation", exception);
        }
    }

    private static void checkReady(Packet packet) throws IOException {
        requireType(packet, UPLOAD_READY);
        requireEnd(packet.body());
    }

    private static void requireType(Packet packet, MessageType expected) throws IOException {
        if (packet.type() == expected) {
            return;
        }
        if (packet.type() == UPLOAD_RESULT || packet.type() == DOWNLOAD_RESULT) {
            checkOnlyResult(packet);
        }
        throw new ProtocolException("Unexpected transfer response type");
    }

    private static void checkOnlyResult(Packet packet) throws IOException {
        var input = packet.body();
        checkResult(input);
        requireEnd(input);
    }

    private static void checkResult(DataInputStream input) throws IOException {
        int code = input.readUnsignedByte();
        if (code == RESULT_OK) {
            return;
        }
        if (code != RESULT_ERROR && code != RESULT_CONFLICT && code != RESULT_NOT_IMPLEMENTED) {
            throw new ProtocolException("Invalid result flag");
        }
        String error = readText(input);
        requireEnd(input);
        throw new Failure(code, error);
    }

    private static void checkCancelled(BooleanSupplier cancelled) throws Cancelled {
        if (cancelled.getAsBoolean()) {
            throw new Cancelled();
        }
    }
}
