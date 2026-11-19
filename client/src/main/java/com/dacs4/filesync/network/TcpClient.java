package com.dacs4.filesync.network;

import com.dacs4.filesync.shared.dto.FileChangeNotification;
import com.dacs4.filesync.shared.exception.ProtocolException;
import com.dacs4.filesync.shared.protocol.FileTransferCodec;
import com.dacs4.filesync.shared.protocol.FileTransferCodec.Packet;
import com.dacs4.filesync.shared.protocol.MessageCodec;
import com.dacs4.filesync.shared.protocol.MessageType;
import com.dacs4.filesync.shared.protocol.ProtocolMessage;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/** Owns the sole reader and serializes all outgoing frames for one TCP session. */
public final class TcpClient implements AutoCloseable {
    private static final int CONNECT_MILLIS = 5_000;
    private static final int REPLY_MILLIS = 15_000;
    private static final ScheduledThreadPoolExecutor WRITE_TIMEOUTS =
        new ScheduledThreadPoolExecutor(1, Thread.ofPlatform().daemon().name("tcp-write-timeout").factory());

    static {
        WRITE_TIMEOUTS.setRemoveOnCancelPolicy(true);
    }

    private final Socket socket = new Socket();
    private final ReentrantLock writer = new ReentrantLock();
    private final ConcurrentMap<UUID, CompletableFuture<Packet>> replies = new ConcurrentHashMap<>();
    private final CompletableFuture<String> welcome = new CompletableFuture<>();
    private volatile CompletableFuture<Void> pong;
    private volatile IOException failure;
    private volatile DataOutputStream output;

    public UUID connect(String address, int port, String name) throws IOException {
        return connect(address, port, name, null);
    }

    public UUID connect(String address, int port, String name,
                        Consumer<FileChangeNotification> fileChanges) throws IOException {
        socket.connect(new InetSocketAddress(address, port), CONNECT_MILLIS);
        socket.setTcpNoDelay(true);
        output = new DataOutputStream(socket.getOutputStream());
        DataInputStream input = new DataInputStream(socket.getInputStream());
        Thread.ofVirtual().name("tcp-client-reader").start(() -> readLoop(input, fileChanges));
        String hello = fileChanges == null ? name : name + FileTransferCodec.FILE_CHANGED_CAPABILITY;
        send(new ProtocolMessage(MessageType.HELLO, hello));
        String id = await(welcome, CONNECT_MILLIS);
        try {
            UUID session = UUID.fromString(id);
            if (!session.toString().equalsIgnoreCase(id)) {
                throw new IllegalArgumentException();
            }
            return session;
        } catch (IllegalArgumentException exception) {
            throw new ProtocolException("Invalid session ID", exception);
        }
    }

    public void ping() throws IOException {
        var reply = new CompletableFuture<Void>();
        pong = reply;
        try {
            send(new ProtocolMessage(MessageType.PING));
            await(reply, REPLY_MILLIS);
        } finally {
            pong = null;
        }
    }

    public Packet exchange(MessageType type, UUID transfer, byte[] data) throws IOException {
        UUID request = UUID.randomUUID();
        var reply = new CompletableFuture<Packet>();
        replies.put(request, reply);
        try {
            send(FileTransferCodec.encode(type, request, transfer, data));
            Packet packet = await(reply, REPLY_MILLIS);
            if (!transfer.equals(packet.transferId())) {
                throw new ProtocolException("Unexpected transfer ID");
            }
            return packet;
        } catch (IOException exception) {
            try {
                close();
            } catch (IOException closeError) {
                exception.addSuppressed(closeError);
            }
            throw exception;
        } finally {
            replies.remove(request);
        }
    }

    public void disconnect() throws IOException {
        // A transfer may own the writer. Closing the socket still ends the session.
        if (output != null && !socket.isClosed() && writer.tryLock()) {
            try {
                write(new ProtocolMessage(MessageType.DISCONNECT));
            } finally {
                writer.unlock();
            }
        }
    }

    private void send(ProtocolMessage message) throws IOException {
        try {
            writer.lockInterruptibly();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("TCP write interrupted");
        }
        try {
            if (failure != null) {
                throw failure;
            }
            write(message);
        } finally {
            writer.unlock();
        }
    }

    private void write(ProtocolMessage message) throws IOException {
        var writing = new java.util.concurrent.atomic.AtomicBoolean(true);
        var timeout = WRITE_TIMEOUTS.schedule(() -> {
            if (writing.getAndSet(false)) {
                fail(new SocketTimeoutException("TCP write timed out"));
            }
        }, REPLY_MILLIS, TimeUnit.MILLISECONDS);
        try {
            MessageCodec.write(output, message);
        } finally {
            writing.set(false);
            timeout.cancel(false);
        }
    }

    private void readLoop(DataInputStream input, Consumer<FileChangeNotification> fileChanges) {
        try {
            for (ProtocolMessage message; (message = MessageCodec.read(input)) != null;) {
                if (message.type() == MessageType.DISCONNECT) {
                    break;
                }
                dispatch(message, fileChanges);
            }
            fail(new EOFException("Server closed the connection"));
        } catch (IOException exception) {
            if (exception instanceof ProtocolException && exception.getCause() instanceof EOFException) {
                fail(new IOException("Connection closed during a frame", exception));
            } else {
                fail(exception);
            }
        }
    }

    private void dispatch(ProtocolMessage message, Consumer<FileChangeNotification> fileChanges)
            throws IOException {
        if (message.type() == MessageType.ERROR) {
            throw new ProtocolException("Server rejected the protocol message");
        }
        if (!welcome.isDone()) {
            if (message.type() != MessageType.WELCOME) {
                throw new ProtocolException("Expected WELCOME");
            }
            welcome.complete(message.payload());
        } else if (message.type() == MessageType.PONG) {
            var reply = pong;
            if (reply == null || !reply.complete(null)) {
                throw new ProtocolException("Unexpected PONG");
            }
        } else if (message.type() == MessageType.FILE_CHANGED) {
            if (fileChanges == null) {
                throw new ProtocolException("File notifications were not requested in HELLO");
            }
            fileChanges.accept(FileTransferCodec.decodeNotification(message));
        } else if (message.type().isFileTransfer()) {
            Packet packet = FileTransferCodec.decode(message);
            var reply = replies.get(packet.requestId());
            if (reply == null || !reply.complete(packet)) {
                throw new ProtocolException("Unexpected response ID");
            }
        } else {
            throw new ProtocolException("Unexpected server message");
        }
    }

    private void fail(IOException exception) {
        failure = exception;
        welcome.completeExceptionally(exception);
        var heartbeat = pong;
        if (heartbeat != null) {
            heartbeat.completeExceptionally(exception);
        }
        replies.values().forEach(reply -> reply.completeExceptionally(exception));
        try {
            close();
        } catch (IOException ignored) {
            // The original failure is already delivered to waiters.
        }
    }

    private static <T> T await(CompletableFuture<T> future, int millis) throws IOException {
        try {
            return future.get(millis, TimeUnit.MILLISECONDS);
        } catch (TimeoutException exception) {
            throw new SocketTimeoutException("Server response timed out");
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException("Request interrupted");
        } catch (ExecutionException exception) {
            if (exception.getCause() instanceof ProtocolException cause) {
                throw new ProtocolException(cause.getMessage(), cause);
            }
            throw new IOException("Connection closed while waiting for a response", exception.getCause());
        }
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }
}
