package com.dacs4.filesync.sync;

import com.dacs4.filesync.network.FileTransferClient.Cancelled;
import com.dacs4.filesync.shared.protocol.FileTransferCodec;
import com.dacs4.filesync.shared.protocol.FileTransferCodec.SafeRoot;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/** Tracks watcher echo fingerprints and session-confirmed server baselines independently. */
public final class SyncFileRegistry {
    private static final int MAX_RECORDS = 2048;
    private static final long TTL_NANOS = TimeUnit.MINUTES.toNanos(10);
    private final LinkedHashMap<String, Entry> records = new LinkedHashMap<>();
    private final Map<String, Baseline> baselines = new HashMap<>();
    private long baselineGeneration;
    private boolean baselineSessionActive;

    public enum Origin { LOCAL, REMOTE }

    public record Fingerprint(String sha256, long size, FileTime modified, FileTime created, Object fileKey) {}

    public static final class FileChangedException extends IOException {
        FileChangedException(Path path) {
            super("File changed while calculating its digest: " + path);
        }
    }

    private record Entry(Fingerprint fingerprint, Origin origin, long expiresAt) {}
    private record Baseline(Optional<String> hash, long generation, boolean valid) {}

    /** Called on session changes. Older baselines require reconciliation before reuse. */
    public synchronized long advanceBaselineGeneration(boolean connected) {
        baselineSessionActive = connected;
        return ++baselineGeneration;
    }

    public synchronized Optional<String> acknowledgedBaseline(String path, long generation) {
        validate(path);
        Baseline baseline = baselines.get(path);
        return baselineSessionActive && generation == baselineGeneration && baseline != null
                && baseline.generation() == generation && baseline.valid()
            ? baseline.hash() : Optional.empty();
    }

    /** Includes old and invalidated baselines, so a replacement save cannot fall back to CREATE. */
    public synchronized boolean hasBaselineRecord(String path) {
        validate(path);
        return baselines.containsKey(path);
    }

    /** A verified old baseline is evidence for a comparison, never a current-session upload permit. */
    synchronized Optional<String> reconciliationBaseline(String path) {
        Baseline baseline = baselines.get(path);
        return baseline != null && baseline.valid() ? baseline.hash() : Optional.empty();
    }

    synchronized void addBaselinePaths(java.util.Set<String> paths, int limit) throws IOException {
        for (String path : baselines.keySet()) {
            if (paths.add(path) && paths.size() > limit) {
                throw new IOException("Reconciliation path limit exceeded");
            }
        }
    }

    synchronized void resetHistory() {
        baselines.replaceAll((path, baseline) -> new Baseline(Optional.empty(), baselineGeneration, false));
        records.clear();
    }

    synchronized void confirmReconciled(String path, String hash, long generation) {
        if (baselineSessionActive && generation == baselineGeneration) {
            baselines.put(path, new Baseline(Optional.of(hash), generation, true));
        }
    }

    public synchronized void recordConfirmedBaseline(String path, String hash, long generation) {
        validate(path);
        if (hash == null || !hash.matches("[0-9A-Fa-f]{64}")) {
            throw new IllegalArgumentException("Invalid acknowledged SHA-256");
        }
        if (!baselineSessionActive || generation != baselineGeneration) {
            return;
        }
        Baseline current = baselines.get(path);
        if (current != null && current.generation() == generation && !current.valid()) {
            // A conflicting observation may have arrived while this CREATE was in flight.
            return;
        }
        baselines.put(path, new Baseline(Optional.of(hash.toLowerCase(Locale.ROOT)), generation, true));
    }

    public synchronized void invalidateBaseline(String path, long generation) {
        validate(path);
        if (baselineSessionActive && generation == baselineGeneration) {
            Baseline current = baselines.get(path);
            Optional<String> anchor = current != null && current.generation() == generation
                ? current.hash() : Optional.empty();
            baselines.put(path, new Baseline(anchor, generation, false));
        }
    }

    synchronized Optional<String> remoteAnchor(String path, long generation) {
        validate(path);
        Baseline current = baselines.get(path);
        return baselineSessionActive && generation == baselineGeneration && current != null
            && current.generation() == generation ? current.hash() : Optional.empty();
    }

    synchronized boolean acknowledgeRemote(String path, String anchor, String hash, long generation) {
        if (remoteAnchor(path, generation).filter(anchor::equals).isEmpty()) {
            return false;
        }
        baselines.put(path, new Baseline(Optional.of(hash), generation, true));
        return true;
    }

    public synchronized void remember(String path, Fingerprint fingerprint, Origin origin) {
        validate(path);
        long now = System.nanoTime();
        expire(now);
        records.remove(path);
        if (records.size() == MAX_RECORDS) {
            records.pollFirstEntry();
        }
        records.put(path, new Entry(fingerprint, origin, now + TTL_NANOS));
    }

    public synchronized boolean contains(String path, Fingerprint fingerprint) {
        validate(path);
        expire(System.nanoTime());
        Entry entry = records.get(path);
        return entry != null && entry.fingerprint().equals(fingerprint);
    }

    public static Fingerprint read(Path file, BooleanSupplier cancelled) throws IOException {
        BasicFileAttributes before = SafeRoot.regularAttributes(file);
        var digest = FileTransferCodec.sha256();
        try (var input = Files.newInputStream(file, LinkOption.NOFOLLOW_LINKS)) {
            byte[] buffer = new byte[FileTransferCodec.CHUNK_BYTES];
            for (int count; (count = input.read(buffer)) != -1;) {
                if (cancelled.getAsBoolean()) {
                    throw new Cancelled();
                }
                digest.update(buffer, 0, count);
            }
        }
        String hash = HexFormat.of().formatHex(digest.digest());
        Fingerprint original = fingerprint(hash, before);
        if (!original.equals(fingerprint(hash, SafeRoot.regularAttributes(file)))) {
            throw new FileChangedException(file);
        }
        if (cancelled.getAsBoolean()) {
            throw new Cancelled();
        }
        return original;
    }

    public static Fingerprint committed(Path file, String verifiedHash) throws IOException {
        return fingerprint(verifiedHash, SafeRoot.regularAttributes(file));
    }

    private static Fingerprint fingerprint(String hash, BasicFileAttributes attributes) {
        return new Fingerprint(hash, attributes.size(), attributes.lastModifiedTime(),
            attributes.creationTime(), attributes.fileKey());
    }

    private void expire(long now) {
        records.values().removeIf(entry -> now - entry.expiresAt() >= 0);
    }

    private static void validate(String path) {
        if (!FileTransferCodec.validRelativePath(path)) {
            throw new IllegalArgumentException("Invalid sync registry path");
        }
    }
}
