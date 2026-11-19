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

import static com.dacs4.filesync.shared.protocol.FileTransferCodec.FILE_CHANGED_CAPABILITY;

public final class ClientHandler implements Runnable, AutoCloseable {
    private static final System.Logger LOG = System.getLogger(ClientHandler.class.getName());

    private final Socket socket;
    private final ClientSessionManager sessions;
    private final FileStorageService storage;
    private final FileNotificationService notifications;
    private final ReentrantLock writer = new ReentrantLock(true);
    private volatile long deadline = System.nanoTime()
        + TimeUnit.MILLISECONDS.toNanos(ServerConfig.HANDSHAKE_TIMEOUT_MILLIS);

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
                boolean receivesPush = name.endsWith(FILE_CHANGED_CAPABILITY);
                if (receivesPush) {
                    name = name.substring(0, name.length() - FILE_CHANGED_CAPABILITY.length());
                    if (name.isBlank()) {
                        throw new ProtocolException("Expected a client name");
                    }
                }
                client = sessions.register(name, remote);
                var sessionId = client.sessionId();
                send(output, new ProtocolMessage(MessageType.WELCOME, sessionId.toString()));
                refreshDeadline();
                if (receivesPush) {
                    subscription = notifications.subscribe(sessionId, frame -> send(output, frame), this::close);
                }
                LOG.log(System.Logger.Level.INFO, "Connected {0} ({1}), session {2}",
                    client.name(), remote, client.sessionId());

                try (var transfers = new FileTransferService(storage,
                        path -> notifications.publishCreated(sessionId, path))) {
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
                LOG.log(System.Logger.Level.WARNING, "Protocol error from {0}: {1}", remote, exception.getMessage());
                send(output, new ProtocolMessage(MessageType.ERROR, exception.getMessage()));
            }
        } catch (IOException exception) {
            LOG.log(System.Logger.Level.DEBUG, "Connection ended for {0}: {1}", remote, exception.getMessage());
        } finally {
            if (subscription != null) {
                subscription.close();
            }
            if (client != null) {
                sessions.remove(client);
                LOG.log(System.Logger.Level.INFO, "Disconnected {0}, session {1}", remote, client.sessionId());
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
            LOG.log(System.Logger.Level.INFO, "Closing inactive connection {0}", socket.getRemoteSocketAddress());
            close();
        }
    }

    @Override
    public void close() {
        try {
            socket.close();
        } catch (IOException exception) {
            LOG.log(System.Logger.Level.DEBUG, "Socket close failed", exception);
        }
    }
}
