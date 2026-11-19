package com.dacs4.filesync.shared.dto;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

public record FileChangeNotification(UUID eventId, Type type, String relativePath, Instant timestamp,
                                     UUID originSessionId, Optional<FileMetadata> metadata) {
    public enum Type { CREATE, MODIFY, DELETE }

    public FileChangeNotification {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(relativePath, "relativePath");
        Objects.requireNonNull(timestamp, "timestamp");
        Objects.requireNonNull(originSessionId, "originSessionId");
        Objects.requireNonNull(metadata, "metadata");
        if (metadata.isPresent() && !relativePath.equals(metadata.get().name())) {
            throw new IllegalArgumentException("Notification path does not match metadata");
        }
    }
}
