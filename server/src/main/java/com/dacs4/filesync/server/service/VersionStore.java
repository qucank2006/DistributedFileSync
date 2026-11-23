package com.dacs4.filesync.server.service;

import com.dacs4.filesync.shared.logging.AppLogging.WarningLimiter;
import com.dacs4.filesync.shared.dto.FileVersion;
import com.dacs4.filesync.shared.protocol.FileTransferCodec;
import com.dacs4.filesync.shared.protocol.FileTransferCodec.SafeRoot;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.HexFormat;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Locale;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static java.nio.file.LinkOption.NOFOLLOW_LINKS;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardOpenOption.*;

/** Private filesystem archive. A sealed pending directory journals each active-file replacement. */
final class VersionStore implements AutoCloseable {
    private static final System.Logger LOG = System.getLogger(VersionStore.class.getName());
    private final WarningLimiter unreadableEntryLog = new WarningLimiter();
    private static final String PREFIX = ".filesync-versions-";
    private static final String STAGE = ".stage-";
    private static final String PENDING = ".pending-";
    private static final String VERSION = "version";
    private static final String CONTENT = "content";
    private static final String METADATA = "metadata.properties";
    private static final String ORIGINAL = "original";
    private static final String REPLACEMENT = "replacement";
    private static final long HISTORY_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(10);

    private final SafeRoot active;
    private final SafeRoot archive;
    private final FileChannel ownership;
    private final FileLock lock;
    private volatile boolean needsRecovery;

    VersionStore(SafeRoot active) throws IOException {
        this.active = active;
        Path path = active.path();
        if (path.getParent() == null) {
            throw new IOException("Active storage must have a parent and a non-reserved name");
        }
        for (Path component : path) {
            if (component.toString().toLowerCase(Locale.ROOT).startsWith(PREFIX)) {
                throw new IOException("Version storage cannot be configured as active storage");
            }
        }
        archive = new SafeRoot(path.resolveSibling(PREFIX + path.getFileName()));
        Path lockFile = archive.path().resolve(".lock");
        if (exists(lockFile)) {
            SafeRoot.regularAttributes(lockFile);
        }
        ownership = FileChannel.open(lockFile, CREATE, WRITE, NOFOLLOW_LINKS);
        try {
            lock = ownership.tryLock();
            if (lock == null) {
                throw new IOException("Version storage is already in use");
            }
            recover();
        } catch (OverlappingFileLockException exception) {
            ownership.close();
            throw new IOException("Version storage is already in use", exception);
        } catch (IOException | RuntimeException exception) {
            try {
                ownership.close();
            } catch (IOException closeFailure) {
                exception.addSuppressed(closeFailure);
            }
            throw exception;
        }
    }

    record Prepared(UUID id, Path directory) {}

    /** Copy the exact conditional base and sync both files before making the journal visible. */
    Prepared prepare(String relative, Path previous, BasicFileAttributes attributes, byte[] expected,
                     Path replacement) throws IOException {
        if (needsRecovery) {
            throw new IOException("Version storage requires recovery before further MODIFY operations");
        }
        archive.check();
        UUID id = UUID.randomUUID();
        Path complete = archive.path().resolve(id.toString());
        Path pending = archive.path().resolve(PENDING + id);
        requireAbsent(complete);
        requireAbsent(pending);
        Path stage = Files.createDirectory(archive.path().resolve(STAGE + id));
        try {
            Path version = Files.createDirectory(stage.resolve(VERSION));
            var digest = FileTransferCodec.sha256();
            long size = 0;
            try (var input = Files.newInputStream(previous, NOFOLLOW_LINKS);
                 var output = FileChannel.open(version.resolve(CONTENT), CREATE_NEW, WRITE, NOFOLLOW_LINKS)) {
                byte[] bytes = new byte[FileTransferCodec.CHUNK_BYTES];
                for (int length; (length = input.read(bytes)) != -1;) {
                    size += length;
                    if (size > attributes.size()) {
                        throw new BaseChanged();
                    }
                    digest.update(bytes, 0, length);
                    ByteBuffer buffer = ByteBuffer.wrap(bytes, 0, length);
                    while (buffer.hasRemaining()) {
                        output.write(buffer);
                    }
                }
                if (size != attributes.size() || !MessageDigest.isEqual(expected, digest.digest())) {
                    throw new BaseChanged();
                }
                output.force(true);
            }
            var metadata = new Properties();
            metadata.setProperty("format", "1");
            metadata.setProperty("id", id.toString());
            metadata.setProperty("path", relative);
            metadata.setProperty("sha256", HexFormat.of().formatHex(expected));
            metadata.setProperty("size", Long.toString(size));
            metadata.setProperty("modifiedMillis", Long.toString(attributes.lastModifiedTime().toMillis()));
            metadata.setProperty("archivedAt", Instant.now().toString());
            try (var output = FileChannel.open(version.resolve(METADATA), CREATE_NEW, WRITE, NOFOLLOW_LINKS)) {
                metadata.store(Channels.newOutputStream(output), null);
                output.force(true);
            }
            try (var upload = FileChannel.open(replacement, WRITE, NOFOLLOW_LINKS)) {
                upload.force(true);
            }
            // These temporary links identify the two files across a process restart.
            linkMarker(stage.resolve(ORIGINAL), previous);
            linkMarker(stage.resolve(REPLACEMENT), replacement);
            archive.check();
            Files.move(stage, pending, ATOMIC_MOVE);
            return new Prepared(id, pending);
        } catch (IOException | RuntimeException exception) {
            try {
                discard(stage);
            } catch (IOException cleanupFailure) {
                exception.addSuppressed(cleanupFailure);
            }
            throw exception;
        }
    }

    static final class BaseChanged extends IOException {}

    private static void linkMarker(Path marker, Path file) throws IOException {
        SafeRoot.regularAttributes(file);
        Files.createLink(marker, file);
        SafeRoot.regularAttributes(marker);
    }

    boolean isPublished(Prepared prepared, Path target) throws IOException {
        archive.check();
        SafeRoot.directoryAttributes(prepared.directory());
        SafeRoot.regularAttributes(target);
        Path replacement = prepared.directory().resolve(REPLACEMENT);
        SafeRoot.regularAttributes(replacement);
        if (Files.isSameFile(target, replacement)) {
            return true;
        }
        Path original = prepared.directory().resolve(ORIGINAL);
        SafeRoot.regularAttributes(original);
        if (Files.isSameFile(target, original)) {
            return false;
        }
        throw new IOException("Cannot determine the outcome of pending version " + prepared.id());
    }

    void requireRecovery() {
        needsRecovery = true;
    }

    void abort(Prepared prepared) throws IOException {
        try {
            // Demote to an unsealed stage before removing anything. Restart can finish an interrupted cleanup.
            archive.check();
            Path stage = archive.path().resolve(STAGE + prepared.id());
            requireAbsent(stage);
            Files.move(prepared.directory(), stage, ATOMIC_MOVE);
            discard(stage);
        } catch (IOException exception) {
            needsRecovery = true;
            throw exception;
        }
    }

    void commit(Prepared prepared) throws IOException {
        try {
            Path pending = prepared.directory();
            archive.check();
            SafeRoot.directoryAttributes(pending);
            Path version = pending.resolve(VERSION);
            SafeRoot.directoryAttributes(version);
            Path complete = archive.path().resolve(prepared.id().toString());
            requireAbsent(complete);
            Files.move(version, complete, ATOMIC_MOVE);
            discard(pending);
        } catch (IOException exception) {
            needsRecovery = true;
            throw exception;
        }
    }

    private void recover() throws IOException {
        try (var pending = Files.newDirectoryStream(archive.path(), PENDING + "*")) {
            for (Path directory : pending) {
                recoverPending(new Prepared(id(directory, PENDING), directory));
            }
        }
        try (var stages = Files.newDirectoryStream(archive.path(), STAGE + "*")) {
            for (Path stage : stages) {
                id(stage, STAGE);
                discard(stage);
            }
        }
    }

    private void recoverPending(Prepared prepared) throws IOException {
        Path pending = prepared.directory();
        SafeRoot.directoryAttributes(pending);
        Path complete = archive.path().resolve(prepared.id().toString());
        Path version = pending.resolve(VERSION);
        if (exists(complete)) {
            requireAbsent(version);
            verifiedPath(prepared.id(), complete);
            discard(pending);
            return;
        }
        String relative = verifiedPath(prepared.id(), version);
        Path target = active.resolve(relative, false);
        if (isPublished(prepared, target)) {
            commit(prepared);
        } else {
            abort(prepared);
        }
    }

    private static String verifiedPath(UUID id, Path version) throws IOException {
        FileVersion metadata = readMetadata(id, version);
        validateContent(version.resolve(CONTENT), metadata, 0);
        return metadata.relativePath();
    }

    FileVersion stageRestore(String relative, UUID id, Path target, long maxBytes) throws IOException {
        archive.check();
        Path entry = archive.path().resolve(id.toString());
        FileVersion metadata = readMetadata(id, entry);
        if (!relative.equals(metadata.relativePath()) || metadata.size() > maxBytes) {
            throw new IOException("Historical version unavailable for this file");
        }
        validateContent(entry.resolve(CONTENT), metadata, 0, target);
        return metadata;
    }

    List<FileVersion> list(String relative) throws IOException {
        archive.check();
        int limit = FileTransferCodec.MAX_LIST_VERSIONS;
        long deadline = System.nanoTime() + HISTORY_TIMEOUT_NANOS;
        var newest = new PriorityQueue<FileVersion>(limit, FileVersion.NEWEST_FIRST.reversed());
        try (var entries = Files.newDirectoryStream(archive.path(),
                entry -> !entry.getFileName().toString().startsWith("."))) {
            for (Path entry : entries) {
                checkHistoryDeadline(deadline);
                try {
                    UUID id = id(entry, "");
                    FileVersion version = readMetadata(id, entry);
                    if (!relative.equals(version.relativePath())
                            || (newest.size() == limit && FileVersion.NEWEST_FIRST.compare(version, newest.element()) >= 0)) {
                        continue;
                    }
                    validateContent(entry.resolve(CONTENT), version, deadline);
                    if (newest.size() == limit) {
                        newest.remove();
                    }
                    newest.add(version);
                } catch (HistoryTimeout exception) {
                    throw exception;
                } catch (IOException exception) {
                    // Skip invalid entries without exposing archive paths or unrelated records.
                    LOG.log(unreadableEntryLog.allow() ? System.Logger.Level.WARNING : System.Logger.Level.DEBUG,
                        "Skipped an unreadable version archive entry", exception);
                }
            }
        } catch (DirectoryIteratorException exception) {
            throw exception.getCause();
        }
        checkHistoryDeadline(deadline);
        return newest.stream().sorted(FileVersion.NEWEST_FIRST).toList();
    }

    static final class HistoryTimeout extends IOException {}

    private static void checkHistoryDeadline(long deadline) throws HistoryTimeout {
        if (Thread.currentThread().isInterrupted() || System.nanoTime() - deadline >= 0) {
            throw new HistoryTimeout();
        }
    }

    private static FileVersion readMetadata(UUID id, Path version) throws IOException {
        SafeRoot.directoryAttributes(version);
        Path metadataFile = version.resolve(METADATA);
        SafeRoot.regularAttributes(metadataFile);
        var metadata = new Properties();
        if (Files.size(metadataFile) > 8192) {
            throw new IOException("Version metadata is too large: " + id);
        }
        long modified;
        long size;
        Instant archivedAt;
        try (var input = Files.newInputStream(metadataFile, NOFOLLOW_LINKS)) {
            metadata.load(input);
            modified = Long.parseLong(metadata.getProperty("modifiedMillis", ""));
            size = Long.parseLong(metadata.getProperty("size", ""));
            archivedAt = Instant.parse(metadata.getProperty("archivedAt", ""));
        } catch (IllegalArgumentException | DateTimeParseException exception) {
            throw new IOException("Invalid version metadata: " + id, exception);
        }
        String relative = metadata.getProperty("path");
        String sha256 = metadata.getProperty("sha256", "");
        if (!"1".equals(metadata.getProperty("format")) || !id.toString().equals(metadata.getProperty("id"))
                || id.equals(FileTransferCodec.NONE) || !FileTransferCodec.validRelativePath(relative)
                || size < 0 || !sha256.matches("[0-9a-f]{64}")) {
            throw new IOException("Invalid version metadata: " + id);
        }
        return new FileVersion(id, relative, sha256, size, modified, archivedAt);
    }

    private static void validateContent(Path content, FileVersion metadata, long deadline) throws IOException {
        validateContent(content, metadata, deadline, null);
    }

    private static void validateContent(Path content, FileVersion metadata, long deadline, Path copy) throws IOException {
        var before = SafeRoot.regularAttributes(content);
        if (before.size() != metadata.size()) {
            throw new IOException("Version content size mismatch");
        }
        var digest = FileTransferCodec.sha256();
        long size = 0;
        try (var input = Files.newInputStream(content, NOFOLLOW_LINKS);
             var output = copy == null ? java.io.OutputStream.nullOutputStream()
                 : Files.newOutputStream(copy, WRITE, TRUNCATE_EXISTING, NOFOLLOW_LINKS)) {
            byte[] bytes = new byte[FileTransferCodec.CHUNK_BYTES];
            for (int length; (length = input.read(bytes)) != -1;) {
                if (deadline != 0) {
                    checkHistoryDeadline(deadline);
                }
                size += length;
                if (size > metadata.size()) {
                    throw new IOException("Version content size mismatch");
                }
                digest.update(bytes, 0, length);
                if (copy != null) {
                    output.write(bytes, 0, length);
                }
            }
        }
        if (size != metadata.size() || !FileStorageService.sameFile(before, SafeRoot.regularAttributes(content))
                || !HexFormat.of().formatHex(digest.digest()).equals(metadata.sha256())) {
            throw new IOException("Pending version failed integrity validation: " + content);
        }
    }

    private void discard(Path directory) throws IOException {
        archive.check();
        if (!exists(directory)) {
            return;
        }
        SafeRoot.directoryAttributes(directory);
        Path version = directory.resolve(VERSION);
        if (exists(version)) {
            SafeRoot.directoryAttributes(version);
            Files.deleteIfExists(version.resolve(CONTENT));
            Files.deleteIfExists(version.resolve(METADATA));
            Files.delete(version);
        }
        Files.deleteIfExists(directory.resolve(ORIGINAL));
        Files.deleteIfExists(directory.resolve(REPLACEMENT));
        Files.delete(directory);
    }

    private static UUID id(Path directory, String prefix) throws IOException {
        String name = directory.getFileName().toString().substring(prefix.length());
        try {
            UUID id = UUID.fromString(name);
            if (id.toString().equals(name)) {
                return id;
            }
        } catch (IllegalArgumentException ignored) {
            // Only names in the archive's reserved transaction format are managed.
        }
        throw new IOException("Invalid version transaction name: " + directory);
    }

    private static void requireAbsent(Path path) throws IOException {
        if (exists(path)) {
            throw new IOException("Version transaction name already exists: " + path);
        }
    }

    private static boolean exists(Path path) throws IOException {
        try {
            Files.readAttributes(path, BasicFileAttributes.class, NOFOLLOW_LINKS);
            return true;
        } catch (NoSuchFileException exception) {
            return false;
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (!ownership.isOpen()) {
            return;
        }
        try (ownership; lock) {
            // Closing the channel releases ownership even if lock release fails.
        }
    }
}
