package com.dacs4.filesync.shared.dto;

/** The name is the slash-separated path relative to server storage. */
public record FileMetadata(String name, long size, long modifiedMillis) {
    public String relativePath() {
        return name;
    }
}
