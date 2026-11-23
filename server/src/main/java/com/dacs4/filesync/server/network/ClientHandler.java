package com.dacs4.filesync.server.network;

import com.dacs4.filesync.server.config.ServerConfig;
import com.dacs4.filesync.server.session.ClientSessionManager;
import com.dacs4.filesync.server.service.FileStorageService;
import com.dacs4.filesync.server.service.FileNotificationService;
import com.dacs4.filesync.server.service.FileTransferService;
import com.dacs4.filesync.shared.dto.ClientInfo;
import com.dacs4.filesync.shared.exception.ProtocolException;
import com.dacs4.filesync.shared.protocol.MessageCodec;
import com.dacs4.filesync.shared.protocol.MessageType;
import com.dacs4.filesync.shared.protocol.ProtocolMessage;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static com.dacs4.filesync.shared.protocol.FileTransferCodec.CONDITIONAL_MODIFY_CAPABILITY;
import static com.dacs4.filesync.shared.protocol.FileTransferCodec.FILE_CHANGED_CAPABILITY;
import static com.dacs4.filesync.shared.protocol.FileTransferCodec.NESTED_SYNC_CAPABILITY;
import static com.dacs4.filesync.shared.protocol.FileTransferCodec.MODIFY_REPLACEMENT_CAPABILITY;
import static com.dacs4.filesync.shared.protocol.FileTransferCodec.RECONCILIATION_CAPABILITY;
import static com.dacs4.filesync.shared.protocol.FileTransferCodec.VERSION_HISTORY_CAPABILITY;
import static com.dacs4.filesync.shared.protocol.FileTransferCodec.RESTORE_CAPABILITY;

public final class ClientHandler implements Runnable, AutoCloseable {
    private static final System.Logger LOG = System.getLogger(ClientHandler.class.getName());

    private final Socket socket;
    private final ClientSessionManager sessions;
    private final FileStorageService storage;
    private final FileNotificationService notifications;
    private final ReentrantLock writer = new ReentrantLock(true);
    private volatile long deadline = System.nanoTime()
        + TimeUnit.MILLISECONDS.toNanos(ServerConfig.HANDSHAKE_TIMEOUT_MILLIS);
    private volatile boolean closeRequested;
    private volatile boolean timedOut;

    ClientHandler(Socket socket, ClientSessionManager sessions, FileStorageService storage,
                  FileNotificationService notifications) {
        this.socket = socket;
        this.sessions = sessions;
        this.storage = storage;
        this.notifications = notifications;
    }

    @Override
    public void run() {
        ClientInfo client = null;
        FileNotificationService.Subscription subscription = null;
        String remote = socket.getRemoteSocketAddress().toString();
        boolean failed = false;
        try (socket) {
            var input = new DataInputStream(socket.getInputStream());
            var output = new DataOutputStream(socket.getOutputStream());
            try {
                ProtocolMessage hello = MessageCodec.read(input);
                if (hello == null) {
                    return;
                }
                if (hello.type() != MessageType.HELLO) {
                    throw new ProtocolException("Expected HELLO");
                }
                String name = hello.payload();
                boolean nested = name.endsWith(NESTED_SYNC_CAPABILITY);
                String suffix = nested ? NESTED_SYNC_CAPABILITY : FILE_CHANGED_CAPABILITY;
                boolean receivesPush = name.endsWith(suffix);
                if (receivesPush) {
                    name = name.substring(0, name.length() - suffix.length());
                }
                boolean conditionalModify = name.endsWith(CONDITIONAL_MODIFY_CAPABILITY);
                if (conditionalModify) {
                    name = name.substring(0, name.length() - CONDITIONAL_MODIFY_CAPABILITY.length());
                }
                boolean modifyReplacement = conditionalModify && name.endsWith(MODIFY_REPLACEMENT_CAPABILITY);
                if (modifyReplacement) {
                    name = name.substring(0, name.length() - MODIFY_REPLACEMENT_CAPABILITY.length());
                }
                boolean reconciliation = nested && modifyReplacement && name.endsWith(RECONCILIATION_CAPABILITY);
                if (reconciliation) {
                    name = name.substring(0, name.length() - RECONCILIATION_CAPABILITY.length());
                }
                boolean versionHistory = name.endsWith(VERSION_HISTORY_CAPABILITY);
                if (versionHistory) {
                    name = name.substring(0, name.length() - VERSION_HISTORY_CAPABILITY.length());
                }
                boolean restore = versionHistory && reconciliation && name.endsWith(RESTORE_CAPABILITY);
                if (restore) {
                    name = name.substring(0, name.length() - RESTORE_CAPABILITY.length());
                }
                if (name.isBlank()) {
                    throw new ProtocolException("Expected a client name");
                }
                client = sessions.register(name, remote);
                var sessionId = client.sessionId();
                send(output, new ProtocolMessage(MessageType.WELCOME,
                    sessionId + (restore ? RESTORE_CAPABILITY : "") + (versionHistory ? VERSION_HISTORY_CAPABILITY : "")
                        + (reconciliation ? RECONCILIATION_CAPABILITY : "")
                        + (modifyReplacement ? MODIFY_REPLACEMENT_CAPABILITY : "")
                        + (conditionalModify ? CONDITIONAL_MODIFY_CAPABILITY : "")));
                refreshDeadline();
                if (receivesPush) {
                    subscription = notifications.subscribe(sessionId, nested, modifyReplacement, restore,
                        frame -> send(output, frame), this::close);
                }
                LOG.log(System.Logger.Level.INFO, "Client connected: {0}", remote);

                try (var transfers = new FileTransferService(storage, nested, conditionalModify, sessionId,
                        (path, hash) -> notifications.publishCreated(sessionId, path, hash),
                        modifyReplacement ? notifications::publishModified : null, reconciliation, versionHistory,
                        restore ? notifications::publishRestored : null)) {
                    for (ProtocolMessage message; (message = MessageCodec.read(input)) != null;) {
                        if (message.type() == MessageType.DISCONNECT) {
                            break;
                        }
                        refreshDeadline();
                        if (message.type() == MessageType.PING) {
                            transfers.expireIfIdle();
                            send(output, new ProtocolMessage(MessageType.PONG));
                        } else if (message.type().isFileTransfer()) {
                            send(output, transfers.handle(message));
                        } else {
                            throw new ProtocolException("Unexpected message from client");
                        }
                    }
                }
            } catch (ProtocolException exception) {
                failed = true;
                LOG.log(System.Logger.Level.WARNING, "Connection ended after a protocol error: " + remote, exception);
                send(output, new ProtocolMessage(MessageType.ERROR, exception.getMessage()));
            }
        } catch (IOException exception) {
            if (!closeRequested && !failed) {
                failed = true;
                LOG.log(System.Logger.Level.WARNING, "Connection failed: " + remote, exception);
            } else {
                LOG.log(System.Logger.Level.DEBUG, "Connection I/O ended during closure", exception);
            }
        } finally {
            if (subscription != null) {
                subscription.close();
            }
            if (client != null) {
                sessions.remove(client);
                if (!failed) {
                    LOG.log(timedOut ? System.Logger.Level.WARNING : System.Logger.Level.INFO,
                        timedOut ? "Client disconnected after inactivity: {0}" : "Client disconnected: {0}", remote);
                }
            }
        }
    }

    private void send(DataOutputStream output, ProtocolMessage message) throws IOException {
        writer.lock();
        try {
            MessageCodec.write(output, message);
        } finally {
            writer.unlock();
        }
    }

    private void refreshDeadline() {
        deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(ServerConfig.INACTIVITY_TIMEOUT_MILLIS);
    }

    public void expireIfIdle(long now) {
        if (!socket.isClosed() && now - deadline >= 0) {
            timedOut = true;
            close();
        }
    }

    @Override
    public void close() {
        closeRequested = true;
        try {
            socket.close();
        } catch (IOException exception) {
            LOG.log(System.Logger.Level.DEBUG, "Socket close failed", exception);
        }
    }
}
