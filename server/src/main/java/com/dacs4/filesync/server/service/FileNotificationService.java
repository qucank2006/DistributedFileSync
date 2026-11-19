package com.dacs4.filesync.server.service;

import com.dacs4.filesync.shared.dto.FileChangeNotification;
import com.dacs4.filesync.shared.dto.FileMetadata;
import com.dacs4.filesync.shared.protocol.FileTransferCodec;
import com.dacs4.filesync.shared.protocol.ProtocolMessage;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class FileNotificationService {
    private static final System.Logger LOG = System.getLogger(FileNotificationService.class.getName());
    private static final int MAX_QUEUED = 32;

    private final FileStorageService storage;
    private final ConcurrentMap<UUID, Subscription> recipients = new ConcurrentHashMap<>();

    public FileNotificationService(FileStorageService storage) {
        this.storage = storage;
    }

    @FunctionalInterface
    public interface Sender {
        void send(ProtocolMessage message) throws IOException;
    }

    public Subscription subscribe(UUID session, Sender sender, Runnable disconnect) {
        Subscription subscription = new Subscription(session, sender, disconnect);
        recipients.put(session, subscription);
        subscription.worker.start();
        return subscription;
    }

    public void publishCreated(UUID origin, Path committedFile) {
        Instant committedAt = Instant.now();
        Optional<FileMetadata> metadata;
        try {
            metadata = Optional.of(storage.metadata(committedFile));
        } catch (IOException | SecurityException exception) {
            LOG.log(System.Logger.Level.DEBUG, "Could not read committed file metadata", exception);
            metadata = Optional.empty();
        }

        var change = new FileChangeNotification(UUID.randomUUID(), FileChangeNotification.Type.CREATE,
            committedFile.getFileName().toString(), committedAt, origin, metadata);
        try {
            ProtocolMessage frame = FileTransferCodec.encodeNotification(change);
            recipients.forEach((session, recipient) -> {
                if (!origin.equals(session)) {
                    recipient.offer(frame);
                }
            });
        } catch (IOException exception) {
            LOG.log(System.Logger.Level.WARNING, "Could not encode committed file notification", exception);
        }
    }

    public final class Subscription implements AutoCloseable {
        private final UUID session;
        private final Sender sender;
        private final Runnable disconnect;
        private final ArrayBlockingQueue<ProtocolMessage> queue = new ArrayBlockingQueue<>(MAX_QUEUED);
        private final Thread worker;
        private volatile boolean closed;

        private Subscription(UUID session, Sender sender, Runnable disconnect) {
            this.session = session;
            this.sender = sender;
            this.disconnect = disconnect;
            worker = Thread.ofVirtual().name("file-notifications-" + session).unstarted(this::deliver);
        }

        private void offer(ProtocolMessage message) {
            if (!closed && !queue.offer(message)) {
                // Queued notifications already invalidate the same server file list.
                LOG.log(System.Logger.Level.DEBUG, "Notification queue full for session {0}", session);
            }
        }

        private void deliver() {
            try {
                while (!closed) {
                    sender.send(queue.take());
                }
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            } catch (IOException exception) {
                if (!closed) {
                    disconnect.run();
                }
            } finally {
                close();
            }
        }

        @Override
        public void close() {
            closed = true;
            recipients.remove(session, this);
            queue.clear();
            if (worker != Thread.currentThread()) {
                worker.interrupt();
            }
        }
    }
}
