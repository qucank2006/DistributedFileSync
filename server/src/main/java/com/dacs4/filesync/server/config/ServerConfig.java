package com.dacs4.filesync.server.config;

public record ServerConfig(String host, int port) {
    public static final int HANDSHAKE_TIMEOUT_MILLIS = 5_000;
    public static final int INACTIVITY_TIMEOUT_MILLIS = 20_000;
    public static final int MAX_CONNECTIONS = 128;

    public ServerConfig {
        if (host == null || host.isBlank() || port < 1 || port > 65_535) {
            throw new IllegalArgumentException("Specify a bind host and a port from 1 to 65535");
        }
    }

    public static ServerConfig fromArgs(String[] args) {
        if (args.length > 2) {
            throw new IllegalArgumentException("Usage: ServerMain [bind-host] [port]");
        }
        return new ServerConfig(args.length > 0 ? args[0] : "0.0.0.0",
            args.length > 1 ? Integer.parseInt(args[1]) : 5000);
    }
}
