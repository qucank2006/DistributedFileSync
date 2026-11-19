package com.dacs4.filesync.server.network;

import com.dacs4.filesync.server.config.ServerConfig;
import com.dacs4.filesync.server.session.ClientSessionManager;
import com.dacs4.filesync.server.service.FileStorageService;
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

public final class ClientHandler implements Runnable, AutoCloseable {
    private static final System.Logger LOG = System.getLogger(ClientHandler.class.getName());

    private final Socket socket;
    private final ClientSessionManager sessions;
    private final FileStorageService storage;
    private volatile long deadline = System.nanoTime()
        + TimeUnit.MILLISECONDS.toNanos(ServerConfig.HANDSHAKE_TIMEOUT_MILLIS);

    ClientHandler(Socket socket, ClientSessionManager sessions, FileStorageService storage) {
        this.socket = socket;
        this.sessions = sessions;
        this.storage = storage;
    }

    @Override
    public void run() {
        ClientInfo client = null;
        String remote = socket.getRemoteSocketAddress().toString();
        try (socket; var transfers = new FileTransferService(storage)) {
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
                client = sessions.register(hello.payload(), remote);
                MessageCodec.write(output, new ProtocolMessage(MessageType.WELCOME, client.sessionId().toString()));
                refreshDeadline();
                LOG.log(System.Logger.Level.INFO, "Connected {0} ({1}), session {2}",
                    client.name(), remote, client.sessionId());

                for (ProtocolMessage message; (message = MessageCodec.read(input)) != null;) {
                    if (message.type() == MessageType.DISCONNECT) {
                        break;
                    }
                    refreshDeadline();
                    if (message.type() == MessageType.PING) {
                        transfers.expireIfIdle();
                        MessageCodec.write(output, new ProtocolMessage(MessageType.PONG));
                    } else if (message.type().isFileTransfer()) {
                        MessageCodec.write(output, transfers.handle(message));
                    } else {
                        throw new ProtocolException("Unexpected message from client");
                    }
                }
            } catch (ProtocolException exception) {
                LOG.log(System.Logger.Level.WARNING, "Protocol error from {0}: {1}", remote, exception.getMessage());
                MessageCodec.write(output, new ProtocolMessage(MessageType.ERROR, exception.getMessage()));
            }
        } catch (IOException exception) {
            LOG.log(System.Logger.Level.DEBUG, "Connection ended for {0}: {1}", remote, exception.getMessage());
        } finally {
            if (client != null) {
                sessions.remove(client);
                LOG.log(System.Logger.Level.INFO, "Disconnected {0}, session {1}", remote, client.sessionId());
            }
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
