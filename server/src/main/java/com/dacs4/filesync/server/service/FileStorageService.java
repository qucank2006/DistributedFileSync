package com.dacs4.filesync.server.service;

import com.dacs4.filesync.shared.dto.FileMetadata;
import com.dacs4.filesync.shared.protocol.FileTransferCodec;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public final class FileStorageService {
    private final Path root;
    private final long maxFileBytes;

    public FileStorageService(Path root, long maxFileBytes) throws IOException {
        this.root = Files.createDirectories(root).toRealPath();
        this.maxFileBytes = maxFileBytes;
    }

    long maxFileBytes() {
        return maxFileBytes;
    }

    Path file(String name) throws IOException {
        if (!FileTransferCodec.validName(name)) {
            throw new IOException("Tên tệp không hợp lệ.");
        }
        return root.resolve(name);
    }

    Path temporaryUpload() throws IOException {
        return Files.createTempFile(root, ".upload-", ".part");
    }

    synchronized void commit(Path temporary, Path target) throws IOException {
        if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
            throw new FileAlreadyExistsException(target.getFileName().toString());
        }
        try {
            Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException exception) {
            Files.move(temporary, target);
        }
    }

    FileMetadata metadata(Path file) throws IOException {
        BasicFileAttributes attributes = Files.readAttributes(file, BasicFileAttributes.class,
            LinkOption.NOFOLLOW_LINKS);
        if (!attributes.isRegularFile()) {
            throw new NoSuchFileException(file.getFileName().toString());
        }
        return new FileMetadata(file.getFileName().toString(), attributes.size(),
            attributes.lastModifiedTime().toMillis());
    }

    List<FileMetadata> list() throws IOException {
        List<FileMetadata> result = new ArrayList<>();
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(root)) {
            for (Path entry : entries) {
                if (!FileTransferCodec.validName(entry.getFileName().toString())) {
                    continue;
                }
                try {
                    result.add(metadata(entry));
                } catch (NoSuchFileException ignored) {
                    continue;
                }
                if (result.size() > FileTransferCodec.MAX_LIST_FILES) {
                    throw new IOException("Thư mục vượt giới hạn danh sách.");
                }
            }
        }
        result.sort(Comparator.comparing(FileMetadata::name));
        return result;
    }
}
