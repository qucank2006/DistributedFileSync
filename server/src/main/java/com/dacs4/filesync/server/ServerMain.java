package com.dacs4.filesync.server;

import com.dacs4.filesync.server.config.ServerConfig;
import com.dacs4.filesync.server.network.TcpServer;

import java.io.IOException;

public final class ServerMain {
    private static final System.Logger LOG = System.getLogger(ServerMain.class.getName());

    private ServerMain() {}

    public static void main(String[] args) {
        try (var server = new TcpServer(ServerConfig.fromArgs(args))) {
            Thread shutdown = new Thread(() -> {
                server.close();
                LOG.log(System.Logger.Level.INFO, "Server stopped");
            }, "server-shutdown");
            Runtime.getRuntime().addShutdownHook(shutdown);
            server.serve();
        } catch (IOException | IllegalArgumentException exception) {
            LOG.log(System.Logger.Level.ERROR, "Server failed: " + exception.getMessage());
            System.exit(1);
        }
    }
}
