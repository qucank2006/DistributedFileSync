package com.dacs4.filesync.server.network;

import com.dacs4.filesync.server.config.ServerConfig;
import com.dacs4.filesync.server.service.HeartbeatMonitor;
import com.dacs4.filesync.server.service.FileStorageService;
import com.dacs4.filesync.server.service.FileNotificationService;
import com.dacs4.filesync.server.session.ClientSessionManager;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public final class TcpServer implements AutoCloseable {
    private static final System.Logger LOG = System.getLogger(TcpServer.class.getName());

    private final ServerSocket listener;
    private final FileStorageService storage;
    private final FileNotificationService notifications;
    private final ExecutorService workers = Executors.newThreadPerTaskExecutor(
        Thread.ofVirtual().uncaughtExceptionHandler((thread, error) ->
            LOG.log(System.Logger.Level.ERROR, "Client handler failed unexpectedly", error)).factory());
    private final Set<ClientHandler> connections = ConcurrentHashMap.newKeySet();
    private final ClientSessionManager sessions = new ClientSessionManager();

    public TcpServer(ServerConfig config) throws IOException {
        storage = new FileStorageService(config.storageRoot(), config.maxFileBytes());
        notifications = new FileNotificationService(storage);
        listener = new ServerSocket();
        try {
            listener.bind(new InetSocketAddress(config.host(), config.port()));
        } catch (IOException | RuntimeException exception) {
            close();
            throw exception;
        }
    }

    public void serve() throws IOException {
        LOG.log(System.Logger.Level.INFO, "Server started on " + listener.getLocalSocketAddress());
        try (var monitor = new HeartbeatMonitor(connections)) {
            while (!listener.isClosed()) {
                Socket socket = listener.accept();
                startClient(socket);
            }
        } catch (IOException exception) {
            if (!listener.isClosed()) {
                throw exception;
            }
        }
    }

    private synchronized void startClient(Socket socket) throws IOException {
        if (listener.isClosed() || connections.size() >= ServerConfig.MAX_CONNECTIONS) {
            socket.close();
            return;
        }
        ClientHandler handler = new ClientHandler(socket, sessions, storage, notifications);
        connections.add(handler);
        workers.execute(() -> {
            try {
                handler.run();
            } finally {
                connections.remove(handler);
            }
        });
    }

    @Override
    public void close() {
        synchronized (this) {
            try {
                listener.close();
            } catch (IOException exception) {
                LOG.log(System.Logger.Level.WARNING, "Listener close failed", exception);
            }
            connections.forEach(ClientHandler::close);
            workers.shutdown();
        }
        try {
            if (!workers.awaitTermination(5, TimeUnit.SECONDS)) {
                workers.shutdownNow();
                if (!workers.awaitTermination(5, TimeUnit.SECONDS)) {
                    LOG.log(System.Logger.Level.WARNING, "Storage remains in use by server workers");
                    return;
                }
            }
            storage.close();
        } catch (InterruptedException exception) {
            workers.shutdownNow();
            Thread.currentThread().interrupt();
        } catch (IOException exception) {
            LOG.log(System.Logger.Level.WARNING, "Version storage close failed", exception);
        }
    }
}
