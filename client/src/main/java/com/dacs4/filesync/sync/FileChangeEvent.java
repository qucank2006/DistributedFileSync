package com.dacs4.filesync.sync;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Objects;
import java.util.OptionalLong;

public record FileChangeEvent(Type type, Path relativePath, Instant timestamp,
                              OptionalLong size, boolean ready) {
    public enum Type { CREATE, MODIFY, DELETE }

    public FileChangeEvent {
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(timestamp, "timestamp");
        Objects.requireNonNull(size, "size");
        validateRelativePath(relativePath);
        if (size.isPresent() && (size.getAsLong() < 0 || type == Type.DELETE)) {
            throw new IllegalArgumentException("Invalid size for " + type);
        }
    }

    static void validateRelativePath(Path path) {
        Objects.requireNonNull(path, "relativePath");
        if (path.isAbsolute() || path.getRoot() != null || path.toString().isEmpty()
                || !path.equals(path.normalize()) || path.startsWith("..") || path.startsWith(".")) {
            throw new IllegalArgumentException("Expected a normalized relative file path: " + path);
        }
    }
}
