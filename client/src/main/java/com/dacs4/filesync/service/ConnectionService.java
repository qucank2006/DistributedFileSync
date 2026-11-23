package com.dacs4.filesync.service;

import com.dacs4.filesync.network.ConnectionState;
import com.dacs4.filesync.network.TcpClient;
import com.dacs4.filesync.shared.dto.FileChangeNotification;
import com.dacs4.filesync.shared.exception.ProtocolException;
import com.dacs4.filesync.shared.logging.AppLogging.WarningLimiter;
import com.dacs4.filesync.sync.SyncTransferManager;
import javafx.application.Platform;
import javafx.beans.property.ReadOnlyObjectProperty;
import javafx.beans.property.ReadOnlyObjectWrapper;

import java.io.IOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/** Public lifecycle methods and observable state belong to the JavaFX application thread. */
public final class ConnectionService implements AutoCloseable {
    private static final System.Logger LOG = System.getLogger(ConnectionService.class.getName());
    private static final int MAX_RETRIES = 3;
    private static final long HEARTBEAT_MILLIS = 5_000;

    public record Status(ConnectionState state, String detail) {}

    private final ReadOnlyObjectWrapper<Status> status = new ReadOnlyObjectWrapper<>(
        new Status(ConnectionState.DISCONNECTED, ""));
    private final ExecutorService worker = Executors.newSingleThreadExecutor(
        Thread.ofVirtual().name("tcp-client").factory());
    private Future<?> task;
    private TcpClient connectedClient;
    private Consumer<FileChangeNotification> fileChangeListener;
    private Runnable localUploadCommittedListener;
    private Consumer<String> historyChangeListener;
    private SyncTransferManager syncTransfers;
    private long generation;
    private boolean closed;

    public ReadOnlyObjectProperty<Status> statusProperty() {
        return status.getReadOnlyProperty();
    }

    public TcpClient connectedClient() {
        return connectedClient;
    }

    public void setFileChangeListener(Consumer<FileChangeNotification> listener) {
        fileChangeListener = listener;
    }

    public void setSyncTransfers(SyncTransferManager manager) {
        if (syncTransfers != null) {
            syncTransfers.setUploadCommittedListener(null);
            syncTransfers.setRemoteModifyAppliedListener(null);
            syncTransfers.setSession(null);
        }
        syncTransfers = manager;
        if (manager != null) {
            Consumer<TcpClient> refreshAfterCommit = client -> Platform.runLater(() -> {
                if (syncTransfers == manager && client == connectedClient && client.isOpen()) {
                    if (localUploadCommittedListener != null) {
                        localUploadCommittedListener.run();
                    }
                    if (historyChangeListener != null) {
                        historyChangeListener.accept(null);
                    }
                }
            });
            manager.setUploadCommittedListener(refreshAfterCommit);
            manager.setRemoteModifyAppliedListener(refreshAfterCommit);
        }
        updateSyncSession();
    }

    public void setLocalUploadCommittedListener(Runnable listener) {
        localUploadCommittedListener = listener;
    }

    public void setHistoryChangeListener(Consumer<String> listener) {
        historyChangeListener = listener;
    }

    private void updateSyncSession() {
        if (syncTransfers != null) {
            syncTransfers.setSession(connectedClient);
        }
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
        if (connectedClient != null) {
            LOG.log(System.Logger.Level.INFO, "Disconnected from server");
        }
        connectedClient = null;
        updateSyncSession();
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
                    var inbox = new NotificationInbox(attempt, client);
                    UUID session = client.connect(address, port, "DistributedFileSync", inbox::offer);
                    boolean reconnected = wasConnected;
                    wasConnected = true;
                    Platform.runLater(() -> {
                        if (attempt == generation) {
                            connectedClient = client;
                            updateSyncSession();
                            status.set(new Status(ConnectionState.CONNECTED, address + ":" + port + " · Phiên " + session));
                            LOG.log(System.Logger.Level.INFO, (reconnected ? "Reconnected to " : "Connected to ")
                                + client.serverEndpoint().getHostString() + ":" + port);
                            inbox.activate();
                        }
                    });
                    try {
                        while (true) {
                            Thread.sleep(HEARTBEAT_MILLIS);
                            client.ping();
                            retries = 0;
                        }
                    } catch (InterruptedException exception) {
                        try {
                            client.disconnect();
                        } catch (IOException exceptionDuringClose) {
                            // Closing the socket also invalidates the server session.
                            LOG.log(System.Logger.Level.DEBUG, "Disconnect notice could not be sent", exceptionDuringClose);
                        }
                        return;
                    } catch (java.io.InterruptedIOException exception) {
                        if (!Thread.currentThread().isInterrupted()) {
                            throw exception;
                        }
                        // Closing the socket releases the reader and any transfer waiters.
                        return;
                    }
                } catch (IOException exception) {
                    if (Thread.currentThread().isInterrupted()) {
                        return;
                    }
                    if (!wasConnected || exception instanceof ProtocolException || retries == MAX_RETRIES) {
                        publish(attempt, ConnectionState.ERROR, errorMessage(exception), exception);
                        return;
                    }
                    retries++;
                    LOG.log(System.Logger.Level.DEBUG, "Scheduling reconnect attempt " + retries, exception);
                    publish(attempt, ConnectionState.RECONNECTING,
                        "Thử kết nối lại (" + retries + "/" + MAX_RETRIES + ") · " + errorMessage(exception), exception);
                    Thread.sleep(1_000L * retries);
                }
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException exception) {
            LOG.log(System.Logger.Level.ERROR, "Connection worker failed unexpectedly", exception);
            throw exception;
        }
    }

    private void publish(long attempt, ConnectionState state, String detail, IOException failure) {
        Platform.runLater(() -> {
            if (attempt == generation) {
                if (state == ConnectionState.ERROR) {
                    String event = stateMessage(status.get().state());
                    LOG.log(System.Logger.Level.ERROR, event + ": " + detail, failure);
                } else if (connectedClient != null) {
                    LOG.log(System.Logger.Level.WARNING, "Connection lost; reconnecting", failure);
                }
                connectedClient = null;
                updateSyncSession();
                status.set(new Status(state, detail));
            }
        });
    }

    private static String stateMessage(ConnectionState previous) {
        return switch (previous) {
            case CONNECTED -> "Connection lost";
            case RECONNECTING -> "Reconnection failed";
            default -> "Connection failed";
        };
    }

    /** A bounded bridge preserves individual creates and coalesces only the list invalidation. */
    private final class NotificationInbox {
        private final long attempt;
        private final TcpClient client;
        private final ArrayBlockingQueue<FileChangeNotification> events = new ArrayBlockingQueue<>(128);
        private final AtomicBoolean posted = new AtomicBoolean();
        private final AtomicBoolean dropped = new AtomicBoolean();
        private final WarningLimiter overflowLog = new WarningLimiter();
        private volatile boolean active;

        NotificationInbox(long attempt, TcpClient client) {
            this.attempt = attempt;
            this.client = client;
        }

        void offer(FileChangeNotification event) {
            if (!events.offer(event)) {
                dropped.set(true);
                if (overflowLog.allow()) {
                    LOG.log(System.Logger.Level.WARNING, "Sync notification inbox full; notifications skipped");
                }
            }
            schedule();
        }

        void activate() {
            active = true;
            schedule();
        }

        private void schedule() {
            if (active && posted.compareAndSet(false, true)) {
                Platform.runLater(this::drain);
            }
        }

        private void drain() {
            try {
                if (attempt != generation || client != connectedClient || !client.isOpen()) {
                    active = false;
                    events.clear();
                    return;
                }
                FileChangeNotification latest = null;
                for (int count = 0; count < 32; count++) {
                    FileChangeNotification event = events.poll();
                    if (event == null) {
                        break;
                    }
                    if (historyChangeListener != null) {
                        historyChangeListener.accept(event.relativePath());
                    }
                    if (syncTransfers != null) {
                        syncTransfers.onRemoteChange(client, event);
                    }
                    latest = event;
                }
                if (latest != null && fileChangeListener != null) {
                    fileChangeListener.accept(latest);
                }
                if (dropped.getAndSet(false) && syncTransfers != null) {
                    syncTransfers.requestReconciliation(client);
                }
            } finally {
                posted.set(false);
                if (active && (!events.isEmpty() || dropped.get())) {
                    schedule();
                }
            }
        }
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
        fileChangeListener = null;
        localUploadCommittedListener = null;
        historyChangeListener = null;
        disconnect();
        setSyncTransfers(null);
        worker.shutdownNow();
    }
}
