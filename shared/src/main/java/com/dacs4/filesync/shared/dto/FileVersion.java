package com.dacs4.filesync.shared.dto;

import java.time.Instant;
import java.util.Comparator;
import java.util.UUID;

public record FileVersion(UUID id, String relativePath, String sha256, long size,
                          long modifiedMillis, Instant archivedAt) {
    public static final Comparator<FileVersion> NEWEST_FIRST =
        Comparator.comparing(FileVersion::archivedAt).reversed().thenComparing(FileVersion::id);
}
