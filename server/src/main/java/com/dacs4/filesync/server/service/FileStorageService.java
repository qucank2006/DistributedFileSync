package com.dacs4.filesync.server.service;

import com.dacs4.filesync.shared.dto.FileMetadata;
import com.dacs4.filesync.shared.dto.FileVersion;
import com.dacs4.filesync.shared.protocol.FileTransferCodec;
import com.dacs4.filesync.shared.protocol.FileTransferCodec.SafeRoot;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.security.MessageDigest;
import java.util.concurrent.locks.ReentrantLock;

public final class FileStorageService implements AutoCloseable {
    private static final System.Logger LOG = System.getLogger(FileStorageService.class.getName());
    private final ReentrantLock[] commitLocks = new ReentrantLock[256];
    private final SafeRoot root;
    private final VersionStore versions;
    private final AtomicReplacement replacement;
    private final long maxFileBytes;

    public FileStorageService(Path root, long maxFileBytes) throws IOException {
        this(root, maxFileBytes,
            (source, target) -> Files.move(source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING));
    }

    @FunctionalInterface
    interface AtomicReplacement {
        void move(Path source, Path target) throws IOException;
    }

    FileStorageService(Path root, long maxFileBytes, AtomicReplacement replacement) throws IOException {
        this.root = new SafeRoot(root);
        this.versions = new VersionStore(this.root);
        this.replacement = replacement;
        this.maxFileBytes = maxFileBytes;
        java.util.Arrays.setAll(commitLocks, index -> new ReentrantLock());
    }

    long maxFileBytes() {
        return maxFileBytes;
    }

    Path file(String relative, boolean createParents) throws IOException {
        return root.resolve(relative, createParents);
    }

    Path temporaryUpload() throws IOException {
        root.check();
        return Files.createTempFile(root.path(), ".upload-", ".part");
    }

    void discard(Path temporary) throws IOException {
        root.check();
        Files.deleteIfExists(temporary);
    }

    Path commit(Path temporary, String relative) throws IOException {
        Path target;
        var lock = commitLock(relative);
        lock.lock();
        try {
            target = file(relative, true);
            root.publish(temporary, relative);
        } finally {
            lock.unlock();
        }
        try {
            discard(temporary);
        } catch (IOException exception) {
            LOG.log(System.Logger.Level.WARNING, "Committed file has a leftover staging link", exception);
        }
        return target;
    }

    private ReentrantLock commitLock(String relative) {
        return commitLocks[Math.floorMod(root.path().resolve(relative).hashCode(), commitLocks.length)];
    }

    boolean matchesBase(String relative, byte[] expected) throws IOException {
        return matchingBase(relative, expected) != null;
    }

    private BasicFileAttributes matchingBase(String relative, byte[] expected) throws IOException {
        if (expected == null || expected.length != FileTransferCodec.HASH_BYTES) {
            throw new IllegalArgumentException("Expected SHA-256 base");
        }
        try {
            var version = readVersion(relative);
            return MessageDigest.isEqual(expected, version.hash()) ? version.attributes() : null;
        } catch (NoSuchFileException | UnstableFileException exception) {
            return null;
        }
    }

    record Version(FileMetadata metadata, byte[] hash) {}
    private record HashedFile(BasicFileAttributes attributes, byte[] hash) {}
    private static final class UnstableFileException extends IOException {}

    Version version(String relative) throws IOException {
        var lock = commitLock(relative);
        lock.lock();
        try {
            var value = readVersion(relative);
            var attributes = value.attributes();
            return new Version(new FileMetadata(relative, attributes.size(), attributes.lastModifiedTime().toMillis()),
                value.hash());
        } catch (NoSuchFileException exception) {
            return null;
        } finally {
            lock.unlock();
        }
    }

    private HashedFile readVersion(String relative) throws IOException {
        Path path = file(relative, false);
        var before = SafeRoot.regularAttributes(path);
        long remaining = before.size();
        if (remaining > maxFileBytes) {
            throw new IOException("File exceeds the configured size limit");
        }
        var digest = FileTransferCodec.sha256();
        try (var input = Files.newInputStream(path, LinkOption.NOFOLLOW_LINKS)) {
            byte[] buffer = new byte[FileTransferCodec.CHUNK_BYTES];
            while (remaining > 0) {
                int count = input.read(buffer, 0, (int) Math.min(remaining, buffer.length));
                if (count < 0) {
                    throw new UnstableFileException();
                }
                digest.update(buffer, 0, count);
                remaining -= count;
            }
            if (input.read() != -1) {
                throw new UnstableFileException();
            }
        }
        var after = SafeRoot.regularAttributes(file(relative, false));
        if (!sameFile(before, after)) {
            throw new UnstableFileException();
        }
        return new HashedFile(after, digest.digest());
    }

    static boolean sameFile(BasicFileAttributes before, BasicFileAttributes after) {
        return before.size() == after.size() && before.lastModifiedTime().equals(after.lastModifiedTime())
            && before.creationTime().equals(after.creationTime())
            && java.util.Objects.equals(before.fileKey(), after.fileKey());
    }

    List<FileVersion> listVersions(String relative) throws IOException {
        try {
            SafeRoot.regularAttributes(file(relative, false));
        } catch (NoSuchFileException exception) {
            return List.of();
        }
        return versions.list(relative);
    }

    FileVersion restore(String relative, UUID id, byte[] expected) throws IOException {
        if (!FileTransferCodec.validRelativePath(relative) || id == null || id.equals(FileTransferCodec.NONE)) {
            throw new IOException("Invalid historical version");
        }
        var lock = commitLock(relative);
        lock.lock();
        try {
            if (!matchesBase(relative, expected)) {
                return null;
            }
            Path staged = temporaryUpload();
            try {
                FileVersion selected = versions.stageRestore(relative, id, staged, maxFileBytes);
                return replace(staged, relative, expected) == null ? null : selected;
            } finally {
                try {
                    discard(staged);
                } catch (IOException exception) {
                    LOG.log(System.Logger.Level.WARNING, "Could not clean up restore staging file", exception);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /** Returns null on a base conflict. Atomic move is the only permitted publication. */
    Path replace(Path temporary, String relative, byte[] expected) throws IOException {
        if (expected == null || expected.length != FileTransferCodec.HASH_BYTES) {
            throw new IllegalArgumentException("Expected SHA-256 base");
        }
        var lock = commitLock(relative);
        lock.lock();
        try {
            if (!root.path().equals(temporary.getParent())) {
                throw new IOException("Temporary file is outside the staging root");
            }
            SafeRoot.regularAttributes(temporary);
            Path target;
            BasicFileAttributes before;
            try {
                target = file(relative, false);
                before = SafeRoot.regularAttributes(target);
            } catch (NoSuchFileException exception) {
                return null;
            }
            if (before.size() > maxFileBytes) {
                throw new IOException("File exceeds the configured size limit");
            }
            VersionStore.Prepared prepared;
            try {
                prepared = versions.prepare(relative, target, before, expected, temporary);
            } catch (VersionStore.BaseChanged exception) {
                return null;
            }
            boolean moved = false;
            boolean outcomeUnknown = false;
            try {
                try {
                    target = file(relative, false);
                    if (!sameFile(before, SafeRoot.regularAttributes(target))) {
                        return null;
                    }
                } catch (NoSuchFileException exception) {
                    return null;
                }
                if (Thread.currentThread().isInterrupted()) {
                    throw new java.io.InterruptedIOException("Replacement interrupted before publication");
                }
                try {
                    replacement.move(temporary, target);
                    moved = true;
                } catch (IOException exception) {
                    try {
                        moved = versions.isPublished(prepared, target);
                    } catch (IOException inspectionFailure) {
                        outcomeUnknown = true;
                        exception.addSuppressed(inspectionFailure);
                    }
                    if (!moved) {
                        throw exception;
                    }
                }
            } finally {
                if (outcomeUnknown) {
                    versions.requireRecovery();
                    LOG.log(System.Logger.Level.ERROR, "Cannot determine MODIFY outcome until restart recovery");
                } else if (!moved) {
                    try {
                        versions.abort(prepared);
                    } catch (IOException exception) {
                        LOG.log(System.Logger.Level.ERROR, "Version rollback requires restart recovery", exception);
                    }
                }
            }
            try {
                versions.commit(prepared);
            } catch (IOException exception) {
                // The active rename has committed. The sealed archive is recoverable and must be retained.
                LOG.log(System.Logger.Level.ERROR, "Version publication requires restart recovery", exception);
            }
            return target;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        versions.close();
    }

    String relativePath(Path file) throws IOException {
        return FileTransferCodec.relativePath(root.path().relativize(file));
    }

    FileMetadata metadata(Path file) throws IOException {
        String relative = relativePath(file);
        BasicFileAttributes attributes = SafeRoot.regularAttributes(root.resolve(relative, false));
        return new FileMetadata(relative, attributes.size(), attributes.lastModifiedTime().toMillis());
    }

    List<FileMetadata> list(boolean nested) throws IOException {
        root.check();
        List<FileMetadata> result = new ArrayList<>();
        int depth = nested ? FileTransferCodec.MAX_PATH_COMPONENTS : 1;
        Files.walkFileTree(root.path(), java.util.Set.of(), depth, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path directory, BasicFileAttributes attributes) throws IOException {
                if (!directory.equals(root.path())) {
                    try {
                        relativePath(directory);
                        SafeRoot.directoryAttributes(directory);
                    } catch (IOException exception) {
                        return FileVisitResult.SKIP_SUBTREE;
                    }
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attributes) throws IOException {
                if (!attributes.isRegularFile() || attributes.isSymbolicLink() || attributes.isOther()) {
                    return FileVisitResult.CONTINUE;
                }
                try {
                    result.add(metadata(file));
                } catch (IOException ignored) {
                    return FileVisitResult.CONTINUE;
                }
                if (result.size() > FileTransferCodec.MAX_LIST_FILES) {
                    throw new IOException("Thư mục vượt giới hạn danh sách.");
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exception) throws IOException {
                if (file.equals(root.path())) {
                    throw exception;
                }
                return FileVisitResult.CONTINUE;
            }
        });
        result.sort(Comparator.comparing(FileMetadata::relativePath));
        return result;
    }
}
