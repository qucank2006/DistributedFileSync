package com.dacs4.filesync.shared.dto;

/** Metadata for one server file. modifiedMillis is Unix epoch milliseconds. */
public record FileMetadata(String name, long size, long modifiedMillis) {}
