package com.dacs4.filesync.server.service;

import com.dacs4.filesync.shared.dto.FileChangeNotification;
import com.dacs4.filesync.shared.dto.FileMetadata;
import com.dacs4.filesync.shared.protocol.FileTransferCodec;
import com.dacs4.filesync.shared.protocol.ProtocolMessage;
import com.dacs4.filesync.shared.logging.AppLogging.WarningLimiter;

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
    private final WarningLimiter overflowLog = new WarningLimiter();

    public FileNotificationService(FileStorageService storage) {
        this.storage = storage;
    }

    @FunctionalInterface
    public interface Sender {
        void send(ProtocolMessage message) throws IOException;
    }

    public Subscription subscribe(UUID session, boolean nested, boolean modifyReplacement,
                                  Sender sender, Runnable disconnect) {
        return subscribe(session, nested, modifyReplacement, false, sender, disconnect);
    }

    public Subscription subscribe(UUID session, boolean nested, boolean modifyReplacement, boolean restore,
                                  Sender sender, Runnable disconnect) {
        Subscription subscription = new Subscription(session, nested, modifyReplacement, restore, sender, disconnect);
        recipients.put(session, subscription);
        subscription.worker.start();
        return subscription;
    }

    public void publishCreated(UUID origin, Path committedFile, String hash) {
        Instant committedAt = Instant.now();
        try {
            String relative = storage.relativePath(committedFile);
            Optional<FileMetadata> metadata;
            try {
                metadata = Optional.of(storage.metadata(committedFile));
            } catch (IOException | SecurityException exception) {
                LOG.log(System.Logger.Level.DEBUG, "Could not read committed file metadata", exception);
                metadata = Optional.empty();
            }
            UUID event = UUID.randomUUID();
            var type = FileChangeNotification.Type.CREATE;
            ProtocolMessage updated = FileTransferCodec.encodeNotification(
                new FileChangeNotification(event, type, relative, committedAt, origin, metadata, Optional.of(hash)));
            ProtocolMessage legacy = relative.contains("/") ? null : FileTransferCodec.encodeNotification(
                new FileChangeNotification(event, type, relative, committedAt, origin, metadata));
            recipients.forEach((session, recipient) -> {
                ProtocolMessage frame = recipient.nested ? updated : legacy;
                if (!origin.equals(session) && frame != null) {
                    recipient.offer(frame);
                }
            });
        } catch (IOException exception) {
            LOG.log(System.Logger.Level.WARNING, "Could not encode committed file notification", exception);
        }
    }

    public void publishModified(FileChangeNotification change) {
        publishModification(change, false);
    }

    public void publishRestored(FileChangeNotification change) {
        publishModification(change, true);
    }

    private void publishModification(FileChangeNotification change, boolean restore) {
        try {
            boolean sameContent = change.previousSha256().isPresent() && change.previousSha256().equals(change.sha256());
            ProtocolMessage full = FileTransferCodec.encodeNotification(change);
            ProtocolMessage nested = FileTransferCodec.encodeNotification(new FileChangeNotification(
                change.eventId(), change.type(), change.relativePath(), change.timestamp(),
                change.originSessionId(), change.metadata(), change.sha256()));
            ProtocolMessage flat = change.relativePath().contains("/") ? null
                : FileTransferCodec.encodeNotification(new FileChangeNotification(
                    change.eventId(), change.type(), change.relativePath(), change.timestamp(),
                    change.originSessionId(), change.metadata()));
            recipients.forEach((session, recipient) -> {
                ProtocolMessage frame = recipient.modifyReplacement && (recipient.nested || flat != null)
                    ? full : recipient.nested ? nested : flat;
                if ((restore || !change.originSessionId().equals(session)) && frame != null
                        && (!sameContent || recipient.restore)) {
                    recipient.offer(frame);
                }
            });
        } catch (IOException exception) {
            LOG.log(System.Logger.Level.WARNING, "Could not encode committed MODIFY notification", exception);
        }
    }

    public final class Subscription implements AutoCloseable {
        private final UUID session;
        private final Sender sender;
        private final boolean nested;
        private final boolean modifyReplacement;
        private final boolean restore;
        private final Runnable disconnect;
        private final ArrayBlockingQueue<ProtocolMessage> queue = new ArrayBlockingQueue<>(MAX_QUEUED);
        private final Thread worker;
        private volatile boolean closed;

        private Subscription(UUID session, boolean nested, boolean modifyReplacement, boolean restore,
                             Sender sender, Runnable disconnect) {
            this.session = session;
            this.sender = sender;
            this.nested = nested;
            this.modifyReplacement = modifyReplacement;
            this.restore = restore;
            this.disconnect = disconnect;
            worker = Thread.ofVirtual().name("file-notifications-" + session).unstarted(this::deliver);
        }

        private void offer(ProtocolMessage message) {
            if (!closed && !queue.offer(message)) {
                // Queued notifications already invalidate the same server file list.
                if (overflowLog.allow()) {
                    LOG.log(System.Logger.Level.WARNING, "Server notification queue full; updates skipped");
                }
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
                    LOG.log(System.Logger.Level.DEBUG, "File notification delivery failed", exception);
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
