package com.dacs4.filesync.sync;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;

public final class SyncCoordinator implements AutoCloseable {
    private static final System.Logger LOG = System.getLogger(SyncCoordinator.class.getName());
    private static final long QUIET_NANOS = TimeUnit.MILLISECONDS.toNanos(500);
    private static final long MAX_WAIT_NANOS = TimeUnit.SECONDS.toNanos(30);
    private static final int MAX_PENDING = 4096;
    private static final int PROBE_BATCH = 128;

    @FunctionalInterface
    public interface Listener {
        void onChange(FileChangeEvent event);
    }

    private final Path root;
    private final Listener listener;
    private Run run;
    private boolean closed;

    public SyncCoordinator(Path root, Listener listener) {
        this.root = Objects.requireNonNull(root);
        this.listener = Objects.requireNonNull(listener);
    }

    public static SyncCoordinator forLocalLogging() {
        String configured = System.getProperty("filesync.sync.root");
        if (configured != null && configured.isBlank()) {
            throw new java.nio.file.InvalidPathException(configured, "Sync root cannot be blank");
        }
        Path root = configured == null
            ? Path.of(System.getProperty("user.home"), "DistributedFileSync", "sync")
            : Path.of(configured);
        return new SyncCoordinator(root, event -> LOG.log(System.Logger.Level.INFO,
            "Local {0}: {1} | size={2} | ready={3} | timestamp={4}",
            event.type(), event.relativePath(),
            event.size().isPresent() ? event.size().getAsLong() : "unavailable",
            event.ready(), event.timestamp()));
    }

    public synchronized void start() {
        if (closed) {
            throw new IllegalStateException("Coordinator is closed");
        }
        if (run == null) {
            run = new Run();
            run.start();
        }
    }

    public synchronized void stop() {
        if (run != null) {
            run.stop();
            run = null;
        }
    }

    @Override
    public synchronized void close() {
        closed = true;
        stop();
    }

    private record Pending(FileChangeEvent event, long firstSeen, long probeAt,
                           LocalFolderWatcher.Stamp previous) {}

    private final class Run {
        private final Map<Path, Pending> pending = new LinkedHashMap<>();
        private final LocalFolderWatcher watcher = new LocalFolderWatcher(root, this::offer);
        private final Thread worker = Thread.ofVirtual().name("local-sync-coordinator").unstarted(this::process);
        private volatile boolean stopped;

        void start() {
            worker.start();
            watcher.start();
        }

        void stop() {
            synchronized (pending) {
                stopped = true;
                pending.clear();
                pending.notifyAll();
            }
            watcher.close();
            worker.interrupt();
        }

        private void offer(FileChangeEvent event) {
            Path path = event.relativePath();
            synchronized (pending) {
                try {
                    // Backpressure applies to the watcher thread only. WatchService overflow triggers a rescan.
                    while (!stopped && pending.size() >= MAX_PENDING && !pending.containsKey(path)) {
                        pending.wait();
                    }
                    if (stopped) {
                        return;
                    }
                    Pending previous = pending.get(path);
                    if (previous != null && previous.event().type() == FileChangeEvent.Type.CREATE
                            && event.type() == FileChangeEvent.Type.MODIFY) {
                        event = new FileChangeEvent(FileChangeEvent.Type.CREATE, path,
                            event.timestamp(), event.size(), false);
                    }
                    long now = System.nanoTime();
                    long firstSeen = previous == null ? now : previous.firstSeen();
                    pending.put(path, new Pending(event, firstSeen, now + QUIET_NANOS, null));
                    pending.notifyAll();
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        private void process() {
            try {
                while (!stopped) {
                    for (Pending candidate : takeDue()) {
                        inspect(candidate);
                    }
                }
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
        }

        private List<Pending> takeDue() throws InterruptedException {
            synchronized (pending) {
                while (!stopped) {
                    List<Pending> due = new ArrayList<>();
                    long wait = Long.MAX_VALUE;
                    long now = System.nanoTime();
                    for (Pending candidate : pending.values()) {
                        long remaining = Math.min(candidate.probeAt() - now,
                            MAX_WAIT_NANOS - (now - candidate.firstSeen()));
                        if (remaining <= 0) {
                            due.add(candidate);
                            if (due.size() == PROBE_BATCH) {
                                break;
                            }
                        } else {
                            wait = Math.min(wait, remaining);
                        }
                    }
                    if (!due.isEmpty()) {
                        return due;
                    }
                    if (pending.isEmpty()) {
                        pending.wait();
                    } else {
                        TimeUnit.NANOSECONDS.timedWait(pending, wait);
                    }
                }
                return List.of();
            }
        }

        private void inspect(Pending candidate) {
            if (stopped) {
                return;
            }
            FileChangeEvent event = candidate.event();
            Path path = event.relativePath();
            long now = System.nanoTime();
            LocalFolderWatcher.Stamp current = null;
            boolean inspected = false;
            try {
                current = watcher.probe(path);
                inspected = true;
            } catch (IOException | SecurityException exception) {
                LOG.log(System.Logger.Level.DEBUG, "File metadata temporarily unavailable for " + path, exception);
            }

            boolean settled = now - candidate.probeAt() >= 0 && inspected
                && (current == null || current.equals(candidate.previous()));
            boolean expired = now - candidate.firstSeen() >= MAX_WAIT_NANOS;
            FileChangeEvent readyEvent;
            synchronized (pending) {
                if (stopped || pending.get(path) != candidate) {
                    return;
                }
                if (!settled && !expired) {
                    pending.put(path, new Pending(event, candidate.firstSeen(), now + QUIET_NANOS, current));
                    return;
                }
                pending.remove(path);
                pending.notifyAll();
                FileChangeEvent.Type type = event.type();
                if (settled && current == null) {
                    type = FileChangeEvent.Type.DELETE;
                } else if (current != null && type == FileChangeEvent.Type.DELETE) {
                    type = FileChangeEvent.Type.CREATE;
                }
                readyEvent = new FileChangeEvent(type, path, event.timestamp(),
                    current == null ? OptionalLong.empty() : OptionalLong.of(current.size()), settled);
            }
            if (!settled) {
                LOG.log(System.Logger.Level.WARNING, "File did not stabilize within 30 seconds: {0}", path);
            }
            if (!stopped) {
                try {
                    listener.onChange(readyEvent);
                } catch (RuntimeException exception) {
                    LOG.log(System.Logger.Level.WARNING, "Local change callback failed for " + path, exception);
                }
            }
        }
    }
}
