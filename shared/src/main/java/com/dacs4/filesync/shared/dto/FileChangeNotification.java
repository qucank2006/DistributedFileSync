package com.dacs4.filesync.shared.dto;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

public record FileChangeNotification(UUID eventId, Type type, String relativePath, Instant timestamp,
                                     UUID originSessionId, Optional<FileMetadata> metadata,
                                     Optional<String> sha256, Optional<String> previousSha256) {
    public enum Type { CREATE, MODIFY, DELETE }

    public FileChangeNotification(UUID eventId, Type type, String relativePath, Instant timestamp,
                                  UUID originSessionId, Optional<FileMetadata> metadata) {
        this(eventId, type, relativePath, timestamp, originSessionId, metadata, Optional.empty());
    }

    public FileChangeNotification(UUID eventId, Type type, String relativePath, Instant timestamp,
                                  UUID originSessionId, Optional<FileMetadata> metadata, Optional<String> sha256) {
        this(eventId, type, relativePath, timestamp, originSessionId, metadata, sha256, Optional.empty());
    }

    public FileChangeNotification {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(relativePath, "relativePath");
        Objects.requireNonNull(timestamp, "timestamp");
        Objects.requireNonNull(originSessionId, "originSessionId");
        Objects.requireNonNull(metadata, "metadata");
        Objects.requireNonNull(sha256, "sha256");
        Objects.requireNonNull(previousSha256, "previousSha256");
        if (previousSha256.isPresent() && (type != Type.MODIFY || sha256.isEmpty()
                || !previousSha256.get().matches("[0-9a-f]{64}"))) {
            throw new IllegalArgumentException("Invalid previous SHA-256 digest");
        }
        if (sha256.filter(hash -> !hash.matches("[0-9a-f]{64}")).isPresent()) {
            throw new IllegalArgumentException("Invalid SHA-256 digest");
        }
        if (metadata.isPresent() && !relativePath.equals(metadata.get().name())) {
            throw new IllegalArgumentException("Notification path does not match metadata");
        }
    }
}
