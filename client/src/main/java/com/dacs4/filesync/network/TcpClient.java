package com.dacs4.filesync.network;

import com.dacs4.filesync.shared.exception.ProtocolException;
import com.dacs4.filesync.shared.protocol.MessageCodec;
import com.dacs4.filesync.shared.protocol.MessageType;
import com.dacs4.filesync.shared.protocol.ProtocolMessage;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.FilterInputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** A single TCP session. Call from a background virtual thread. */
public final class TcpClient implements AutoCloseable {
    private static final int TIMEOUT_MILLIS = 5_000;

    private final Socket socket = new Socket();
    private DataInputStream input;
    private long readDeadline;
    private DataOutputStream output;

    public UUID connect(String address, int port, String name) throws IOException {
        socket.connect(new InetSocketAddress(address, port), TIMEOUT_MILLIS);
        socket.setTcpNoDelay(true);
        input = new DataInputStream(new FilterInputStream(socket.getInputStream()) {
            @Override
            public int read() throws IOException {
                applyReadTimeout();
                return in.read();
            }

            @Override
            public int read(byte[] bytes, int offset, int length) throws IOException {
                applyReadTimeout();
                return in.read(bytes, offset, length);
            }
        });
        output = new DataOutputStream(socket.getOutputStream());
        MessageCodec.write(output, new ProtocolMessage(MessageType.HELLO, name));
        String id = expect(MessageType.WELCOME).payload();
        try {
            UUID sessionId = UUID.fromString(id);
            if (!sessionId.toString().equalsIgnoreCase(id)) {
                throw new IllegalArgumentException();
            }
            return sessionId;
        } catch (IllegalArgumentException exception) {
            throw new ProtocolException("Invalid session ID", exception);
        }
    }

    public void ping() throws IOException {
        MessageCodec.write(output, new ProtocolMessage(MessageType.PING));
        expect(MessageType.PONG);
    }

    public void disconnect() throws IOException {
        if (output != null && !socket.isClosed()) {
            MessageCodec.write(output, new ProtocolMessage(MessageType.DISCONNECT));
        }
    }

    private ProtocolMessage expect(MessageType expected) throws IOException {
        readDeadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(TIMEOUT_MILLIS);
        ProtocolMessage message = MessageCodec.read(input);
        if (message == null || message.type() == MessageType.DISCONNECT) {
            throw new EOFException("Server closed the connection");
        }
        if (message.type() != expected) {
            throw new ProtocolException("Server did not return " + expected);
        }
        return message;
    }

    private void applyReadTimeout() throws IOException {
        long remaining = readDeadline - System.nanoTime();
        if (remaining <= 0) {
            throw new SocketTimeoutException("Response deadline exceeded");
        }
        socket.setSoTimeout((int) Math.max(1, TimeUnit.NANOSECONDS.toMillis(remaining)));
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }
}
