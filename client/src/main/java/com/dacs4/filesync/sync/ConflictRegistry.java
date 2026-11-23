package com.dacs4.filesync.sync;

import com.dacs4.filesync.shared.protocol.FileTransferCodec;

import java.time.Instant;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

/** In-memory observations. Viewing a record does not change its status. */
public final class ConflictRegistry {
    private static final System.Logger LOG = System.getLogger(ConflictRegistry.class.getName());
    public enum Type { SERVER_BASE_MISMATCH, LOCAL_EDITS, LOCAL_CHANGED_DURING_DOWNLOAD, NOTIFICATION_CHAIN, RECONCILIATION }
    public enum Status { OPEN, RESOLVED, REQUIRES_RECONCILIATION }

    public record Entry(UUID id, String path, Type type, Instant detectedAt, String localSha256,
                        String expectedSha256, String serverSha256, Status status, String explanation) {}

    private record Key(String path, Type type) {}

    private final Map<Key, Entry> entries = new LinkedHashMap<>();
    private final CopyOnWriteArrayList<Runnable> listeners = new CopyOnWriteArrayList<>();

    public synchronized List<Entry> snapshot() {
        return entries.values().stream().sorted(Comparator.comparing(Entry::detectedAt).reversed()).toList();
    }

    public Runnable subscribe(Runnable listener) {
        listeners.add(Objects.requireNonNull(listener));
        return () -> listeners.remove(listener);
    }

    void reportReconciliation(String path, Status status, String localHash, String base,
                              String serverHash, String explanation) {
        Type type;
        synchronized (this) {
            type = entries.values().stream().filter(entry -> entry.path().equals(path)
                    && entry.status() != Status.RESOLVED).map(Entry::type).findFirst().orElse(Type.RECONCILIATION);
        }
        report(path, type, status, localHash, base, serverHash, explanation);
    }

    public void report(String path, Type type, Status status, String localSha256, String expectedSha256,
                       String serverSha256, String explanation) {
        if (!FileTransferCodec.validRelativePath(path) || status == Status.RESOLVED) {
            throw new IllegalArgumentException("Invalid unresolved conflict");
        }
        Objects.requireNonNull(type);
        Objects.requireNonNull(status);
        Objects.requireNonNull(explanation);
        synchronized (this) {
            var key = new Key(path, type);
            var previous = entries.get(key);
            if (previous != null && previous.status() == Status.OPEN) {
                status = Status.OPEN;
            }
            var next = new Entry(previous == null ? UUID.randomUUID() : previous.id(), path, type,
                previous == null ? Instant.now() : previous.detectedAt(), localSha256, expectedSha256,
                serverSha256, status, explanation);
            if (next.equals(previous)) {
                return;
            }
            entries.put(key, next);
            if (previous == null || previous.status() != next.status()) {
                LOG.log(System.Logger.Level.WARNING, "Conflict detected: {0} ({1}, {2})", path, type, status);
            }
        }
        for (var listener : listeners) {
            try {
                listener.run();
            } catch (RuntimeException exception) {
                LOG.log(System.Logger.Level.WARNING, "Conflict observer failed", exception);
            }
        }
    }
}
