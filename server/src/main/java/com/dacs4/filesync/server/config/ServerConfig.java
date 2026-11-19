package com.dacs4.filesync.server.config;

import java.nio.file.Path;

public record ServerConfig(String host, int port, Path storageRoot, long maxFileBytes) {
    public static final int HANDSHAKE_TIMEOUT_MILLIS = 5_000;
    public static final int INACTIVITY_TIMEOUT_MILLIS = 20_000;
    public static final int MAX_CONNECTIONS = 128;

    public ServerConfig {
        if (host == null || host.isBlank() || port < 1 || port > 65_535) {
            throw new IllegalArgumentException("Specify a bind host and a port from 1 to 65535");
        }
        if (storageRoot == null || maxFileBytes < 1) {
            throw new IllegalArgumentException("Specify a storage root and a positive file size limit");
        }
    }

    public ServerConfig(String host, int port) {
        this(host, port, Path.of(System.getProperty("filesync.storage.root", "server-storage")),
            Long.parseLong(System.getProperty("filesync.maxFileBytes", "1073741824")));
    }

    public static ServerConfig fromArgs(String[] args) {
        if (args.length > 2) {
            throw new IllegalArgumentException("Usage: ServerMain [bind-host] [port]");
        }
        return new ServerConfig(args.length > 0 ? args[0] : "0.0.0.0",
            args.length > 1 ? Integer.parseInt(args[1]) : 5000);
    }
}
