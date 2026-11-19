package com.dacs4.filesync.service;

import com.dacs4.filesync.network.ConnectionState;
import com.dacs4.filesync.network.TcpClient;
import com.dacs4.filesync.shared.exception.ProtocolException;
import javafx.application.Platform;
import javafx.beans.property.ReadOnlyObjectProperty;
import javafx.beans.property.ReadOnlyObjectWrapper;

import java.io.IOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/** Public lifecycle methods and observable state belong to the JavaFX application thread. */
public final class ConnectionService implements AutoCloseable {
    private static final int MAX_RETRIES = 3;
    private static final long HEARTBEAT_MILLIS = 5_000;

    public record Status(ConnectionState state, String detail) {}

    private final ReadOnlyObjectWrapper<Status> status = new ReadOnlyObjectWrapper<>(
        new Status(ConnectionState.DISCONNECTED, ""));
    private final ExecutorService worker = Executors.newSingleThreadExecutor(
        Thread.ofVirtual().name("tcp-client").factory());
    private Future<?> task;
    private long generation;
    private boolean closed;

    public ReadOnlyObjectProperty<Status> statusProperty() {
        return status.getReadOnlyProperty();
    }

    public void connect(String address, int port) {
        if (closed || status.get().state().active()) {
            return;
        }
        long attempt = ++generation;
        status.set(new Status(ConnectionState.CONNECTING, "Đang kết nối tới " + address + ":" + port + "…"));
        task = worker.submit(() -> runConnection(attempt, address, port));
    }

    public void disconnect() {
        ++generation;
        if (task != null) {
            task.cancel(true);
            task = null;
        }
        status.set(new Status(ConnectionState.DISCONNECTED, ""));
    }

    private void runConnection(long attempt, String address, int port) {
        int retries = 0;
        boolean wasConnected = false;
        try {
            while (!Thread.currentThread().isInterrupted()) {
                try (var client = new TcpClient()) {
                    UUID session = client.connect(address, port, "DistributedFileSync");
                    wasConnected = true;
                    publish(attempt, ConnectionState.CONNECTED, address + ":" + port + " · Phiên " + session);
                    try {
                        while (true) {
                            Thread.sleep(HEARTBEAT_MILLIS);
                            client.ping();
                            retries = 0;
                        }
                    } catch (InterruptedException exception) {
                        try {
                            client.disconnect();
                        } catch (IOException ignored) {
                            // Closing the socket also invalidates the server session.
                        }
                        return;
                    }
                } catch (IOException exception) {
                    if (Thread.currentThread().isInterrupted()) {
                        return;
                    }
                    if (!wasConnected || exception instanceof ProtocolException || retries == MAX_RETRIES) {
                        publish(attempt, ConnectionState.ERROR, errorMessage(exception));
                        return;
                    }
                    retries++;
                    publish(attempt, ConnectionState.RECONNECTING,
                        "Thử kết nối lại (" + retries + "/" + MAX_RETRIES + ") · " + errorMessage(exception));
                    Thread.sleep(1_000L * retries);
                }
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }

    private void publish(long attempt, ConnectionState state, String detail) {
        Platform.runLater(() -> {
            if (attempt == generation) {
                status.set(new Status(state, detail));
            }
        });
    }

    private static String errorMessage(IOException exception) {
        return switch (exception) {
            case ProtocolException ignored -> "Máy chủ trả về thông điệp không tương thích.";
            case SocketTimeoutException ignored -> "Máy chủ không phản hồi đúng hạn.";
            case ConnectException ignored -> "Máy chủ từ chối kết nối. Kiểm tra máy chủ và cổng.";
            case NoRouteToHostException ignored -> "Không tìm được đường tới máy chủ. Kiểm tra địa chỉ và mạng.";
            default -> "Kết nối bị gián đoạn. Kiểm tra mạng và máy chủ.";
        };
    }

    @Override
    public void close() {
        closed = true;
        disconnect();
        worker.shutdownNow();
    }
}
