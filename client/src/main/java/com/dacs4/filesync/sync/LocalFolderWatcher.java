package com.dacs4.filesync.sync;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static java.nio.file.LinkOption.NOFOLLOW_LINKS;
import static java.nio.file.StandardWatchEventKinds.*;

public final class LocalFolderWatcher implements AutoCloseable {
    private static final System.Logger LOG = System.getLogger(LocalFolderWatcher.class.getName());

    private final Path configuredRoot;
    private final Consumer<FileChangeEvent> listener;
    private volatile Thread worker;
    private volatile Path root;
    private boolean closed;

    public LocalFolderWatcher(Path root, Consumer<FileChangeEvent> listener) {
        configuredRoot = Objects.requireNonNull(root).toAbsolutePath().normalize();
        this.listener = Objects.requireNonNull(listener);
    }

    public synchronized void start() {
        if (closed) {
            throw new IllegalStateException("Watcher is closed");
        }
        if (worker == null) {
            root = null;
            worker = Thread.ofVirtual().name("local-folder-watcher").unstarted(this::watch);
            worker.start();
        }
    }

    public synchronized void stop() {
        Thread running = worker;
        worker = null;
        if (running != null) {
            running.interrupt();
        }
    }

    @Override
    public synchronized void close() {
        closed = true;
        stop();
    }

    private boolean running() {
        return worker == Thread.currentThread() && !Thread.currentThread().isInterrupted();
    }

    private void watch() {
        try (WatchService service = configuredRoot.getFileSystem().newWatchService()) {
            if (Files.isSymbolicLink(configuredRoot)) {
                throw new IOException("The sync root cannot be a symbolic link");
            }
            Path directory = Files.createDirectories(configuredRoot).toRealPath();
            synchronized (this) {
                if (!running()) {
                    return;
                }
                root = directory;
            }
            new DirectoryWatch(directory, service).run();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        } catch (IOException | RuntimeException exception) {
            if (running()) {
                LOG.log(System.Logger.Level.ERROR, "Local watcher stopped: " + configuredRoot, exception);
            }
        } finally {
            synchronized (this) {
                if (worker == Thread.currentThread()) {
                    worker = null;
                }
            }
        }
    }

    /** Read metadata only. A missing or non-regular path has no file stamp. */
    Stamp probe(Path relative) throws IOException {
        FileChangeEvent.validateRelativePath(relative);
        Path directory = root;
        if (directory == null) {
            return null;
        }
        return readStamp(directory, relative);
    }

    private static Stamp readStamp(Path directory, Path relative) throws IOException {
        Path file = directory.resolve(relative);
        if (!file.startsWith(directory)) {
            return null;
        }
        try {
            if (!Files.readAttributes(directory, BasicFileAttributes.class, NOFOLLOW_LINKS).isDirectory()
                    || !directory.toRealPath().equals(directory)) {
                return null;
            }
            Path parent = directory;
            for (int index = 0; index < relative.getNameCount() - 1; index++) {
                parent = parent.resolve(relative.getName(index));
                if (!Files.readAttributes(parent, BasicFileAttributes.class, NOFOLLOW_LINKS).isDirectory()) {
                    return null;
                }
            }
            BasicFileAttributes attributes = Files.readAttributes(file, BasicFileAttributes.class, NOFOLLOW_LINKS);
            return attributes.isRegularFile() ? Stamp.from(attributes) : null;
        } catch (NoSuchFileException | NotDirectoryException exception) {
            return null;
        }
    }

    record Stamp(long size, FileTime modified, FileTime created, Object key) {
        static Stamp from(BasicFileAttributes attributes) {
            return new Stamp(attributes.size(), attributes.lastModifiedTime(), attributes.creationTime(), attributes.fileKey());
        }
    }

    private static boolean ignored(Path path) {
        String name = path.getFileName().toString().toLowerCase(Locale.ROOT);
        return name.endsWith(".part") || name.endsWith(".tmp") || name.endsWith(".swp")
            || name.endsWith(".crdownload") || name.startsWith(".download-")
            || name.startsWith(".upload-") || name.startsWith("~$");
    }

    private final class DirectoryWatch {
        private final Path directory;
        private final WatchService service;
        private final Map<WatchKey, Path> directories = new HashMap<>();
        private final Map<Path, WatchKey> registrations = new HashMap<>();
        private Map<Path, Stamp> known = new HashMap<>();

        DirectoryWatch(Path directory, WatchService service) {
            this.directory = directory;
            this.service = service;
        }

        void run() throws IOException, InterruptedException {
            boolean retry = rescan(false);
            if (running()) {
                LOG.log(System.Logger.Level.INFO, "Watching local sync folder {0}", directory);
            }
            long nextRetry = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (running()) {
                WatchKey key = service.poll(1, TimeUnit.SECONDS);
                boolean scan = false;
                if (key != null) {
                    Path parent = directories.get(key);
                    if (parent != null) {
                        for (WatchEvent<?> event : key.pollEvents()) {
                            if (!running()) {
                                return;
                            }
                            if (event.kind() == OVERFLOW) {
                                LOG.log(System.Logger.Level.WARNING, "Watch event overflow; rescanning {0}", directory);
                                scan = true;
                            } else if (event.context() instanceof Path child) {
                                scan |= update(parent.resolve(child).normalize(), event.kind());
                            }
                        }
                    }
                    if (!key.reset() && parent != null) {
                        directories.remove(key);
                        registrations.remove(parent, key);
                        scan = true;
                    }
                }
                if (scan || (retry && System.nanoTime() - nextRetry >= 0)) {
                    retry = rescan(true);
                    nextRetry = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
                }
            }
        }

        private boolean update(Path path, WatchEvent.Kind<?> kind) {
            if (!path.startsWith(directory) || path.equals(directory) || ignored(path)) {
                return false;
            }
            if (registrations.containsKey(path) || Files.isDirectory(path, NOFOLLOW_LINKS)) {
                return kind != ENTRY_MODIFY;
            }
            Path relative = directory.relativize(path);
            try {
                Stamp current = readStamp(directory, relative);
                Stamp previous = current == null ? known.remove(relative) : known.put(relative, current);
                if (current == null) {
                    if (previous != null) {
                        emit(FileChangeEvent.Type.DELETE, relative, null);
                    }
                } else {
                    emit(previous == null ? FileChangeEvent.Type.CREATE : FileChangeEvent.Type.MODIFY, relative, current);
                }
                return false;
            } catch (IOException exception) {
                LOG.log(System.Logger.Level.DEBUG, "Could not inspect " + path, exception);
                return true;
            }
        }

        private boolean rescan(boolean reportChanges) throws IOException {
            Map<Path, Stamp> current = new HashMap<>();
            Set<Path> visited = new HashSet<>();
            Set<Path> uncertain = new HashSet<>();
            boolean scanRoot = false;
            try {
                var attributes = Files.readAttributes(directory, BasicFileAttributes.class, NOFOLLOW_LINKS);
                scanRoot = attributes.isDirectory() && directory.toRealPath().equals(directory);
            } catch (NoSuchFileException | NotDirectoryException ignored) {
                // An absent root is retried and any previously known files are reported deleted.
            } catch (IOException exception) {
                uncertain.add(directory);
                LOG.log(System.Logger.Level.DEBUG, "Could not inspect sync root " + directory, exception);
            }
            if (scanRoot) {
                Files.walkFileTree(directory, new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult preVisitDirectory(Path path, BasicFileAttributes attributes) {
                        if (!running()) {
                            return FileVisitResult.TERMINATE;
                        }
                        if (!path.equals(directory) && ignored(path)) {
                            return FileVisitResult.SKIP_SUBTREE;
                        }
                        visited.add(path);
                        try {
                            register(path);
                        } catch (IOException exception) {
                            uncertain.add(path);
                            LOG.log(System.Logger.Level.DEBUG, "Could not watch " + path, exception);
                        }
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult visitFile(Path path, BasicFileAttributes attributes) {
                        if (!running()) {
                            return FileVisitResult.TERMINATE;
                        }
                        if (!path.equals(directory) && attributes.isRegularFile() && !ignored(path)) {
                            current.put(directory.relativize(path), Stamp.from(attributes));
                        }
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult visitFileFailed(Path path, IOException exception) {
                        if (!(exception instanceof NoSuchFileException)) {
                            uncertain.add(path);
                            LOG.log(System.Logger.Level.DEBUG, "Could not scan " + path, exception);
                        }
                        return running() ? FileVisitResult.CONTINUE : FileVisitResult.TERMINATE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path path, IOException exception) {
                        if (exception != null && !(exception instanceof NoSuchFileException)) {
                            uncertain.add(path);
                            LOG.log(System.Logger.Level.DEBUG, "Could not finish scanning " + path, exception);
                        }
                        return running() ? FileVisitResult.CONTINUE : FileVisitResult.TERMINATE;
                    }
                });
            }
            if (!running()) {
                return false;
            }

            for (var entry : known.entrySet()) {
                Path relative = entry.getKey();
                if (!current.containsKey(relative)) {
                    if (underUncertainPath(directory.resolve(relative), uncertain)) {
                        current.put(relative, entry.getValue());
                    } else if (reportChanges) {
                        emit(FileChangeEvent.Type.DELETE, relative, null);
                    }
                }
            }
            if (reportChanges) {
                for (var entry : current.entrySet()) {
                    Stamp previous = known.get(entry.getKey());
                    if (!entry.getValue().equals(previous)) {
                        emit(previous == null ? FileChangeEvent.Type.CREATE : FileChangeEvent.Type.MODIFY,
                            entry.getKey(), entry.getValue());
                    }
                }
            }
            known = current;
            registrations.entrySet().removeIf(entry -> {
                if (visited.contains(entry.getKey()) || underUncertainPath(entry.getKey(), uncertain)) {
                    return false;
                }
                entry.getValue().cancel();
                directories.remove(entry.getValue());
                return true;
            });
            return !uncertain.isEmpty() || !visited.contains(directory);
        }

        private void register(Path path) throws IOException {
            WatchKey previous = registrations.get(path);
            WatchKey current = path.register(service, ENTRY_CREATE, ENTRY_MODIFY, ENTRY_DELETE);
            if (previous != null && previous != current) {
                // A directory can move within the root while its watch key remains valid.
                if (path.equals(directories.get(previous))) {
                    previous.cancel();
                    directories.remove(previous);
                }
            }
            Path oldPath = directories.put(current, path);
            if (oldPath != null && !oldPath.equals(path)) {
                registrations.remove(oldPath, current);
            }
            registrations.put(path, current);
        }

        private void emit(FileChangeEvent.Type type, Path relative, Stamp stamp) {
            if (running()) {
                listener.accept(new FileChangeEvent(type, relative, Instant.now(),
                    stamp == null ? OptionalLong.empty() : OptionalLong.of(stamp.size()), false));
            }
        }

        private boolean underUncertainPath(Path path, Set<Path> uncertain) {
            for (Path ancestor = path; ancestor != null && ancestor.startsWith(directory);
                    ancestor = ancestor.getParent()) {
                if (uncertain.contains(ancestor)) {
                    return true;
                }
            }
            return false;
        }
    }
}
