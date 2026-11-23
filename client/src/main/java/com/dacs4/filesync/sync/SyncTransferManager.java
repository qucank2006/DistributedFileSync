package com.dacs4.filesync.sync;

import com.dacs4.filesync.shared.logging.AppLogging.WarningLimiter;
import com.dacs4.filesync.network.FileTransferClient;
import com.dacs4.filesync.network.TcpClient;
import com.dacs4.filesync.shared.dto.FileChangeNotification;
import com.dacs4.filesync.shared.dto.FileMetadata;
import com.dacs4.filesync.shared.exception.ProtocolException;
import com.dacs4.filesync.shared.protocol.FileTransferCodec;
import com.dacs4.filesync.shared.protocol.FileTransferCodec.SafeRoot;

import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.InetSocketAddress;
import java.nio.file.*;
import java.time.Instant;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import static com.dacs4.filesync.sync.ConflictRegistry.Status.*;
import static com.dacs4.filesync.sync.ConflictRegistry.Type.*;

public final class SyncTransferManager implements AutoCloseable {
    private static final System.Logger LOG = System.getLogger(SyncTransferManager.class.getName());
    private final WarningLimiter queueLog = new WarningLimiter();
    private final WarningLimiter reconciliationLog = new WarningLimiter();
    private final WarningLimiter reconciliationFailureLog = new WarningLimiter();
    private static final int QUEUE_LIMIT = 128;
    private static final int MAX_ATTEMPTS = 3;
    private static final int MAX_RECONCILIATION_PATHS = 4 * FileTransferCodec.MAX_LIST_FILES;
    private static final FileTransferClient.Progress NO_PROGRESS = (bytes, total) -> {};

    private final Path root;
    private final SyncFileRegistry registry = new SyncFileRegistry();
    private final ConflictRegistry conflicts = new ConflictRegistry();
    private final ThreadPoolExecutor worker = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(QUEUE_LIMIT), Thread.ofPlatform().name("automatic-file-sync")
            .uncaughtExceptionHandler((thread, error) ->
                LOG.log(System.Logger.Level.ERROR, "Sync worker failed unexpectedly", error)).factory());
    private final Set<Job> pending = new HashSet<>();
    private final Set<Job> followUpModifications = new HashSet<>();
    private final Map<Job, List<FileChangeNotification>> remoteChanges = new LinkedHashMap<>();
    private final LinkedHashSet<UUID> receivedChanges = new LinkedHashSet<>();
    private static final int REMOTE_CHAIN_LIMIT = 32;
    private static final int SEEN_CHANGES_LIMIT = 2048;
    private volatile Session session;
    private InetSocketAddress serverEndpoint;
    private final Map<String, FileChangeEvent.Type> localIntents = new LinkedHashMap<>();
    private boolean intentOverflow;
    private Session reconciliationQueued;
    private boolean reconciliationRunning;
    private boolean reconcileAgain;
    private volatile Consumer<TcpClient> uploadCommittedListener;
    private volatile Consumer<TcpClient> remoteModifyAppliedListener;
    private boolean closed;

    private record Session(TcpClient client, Instant started, long baselineGeneration) {}
    private record Job(Session session, String path, boolean download, boolean remoteModify) {}

    public SyncTransferManager(Path root) {
        this.root = root.toAbsolutePath().normalize();
    }

    public ConflictRegistry conflicts() {
        return conflicts;
    }

    public Path localRoot() {
        return root;
    }

    public void setUploadCommittedListener(Consumer<TcpClient> listener) {
        uploadCommittedListener = listener;
    }

    public void setRemoteModifyAppliedListener(Consumer<TcpClient> listener) {
        remoteModifyAppliedListener = listener;
    }

    public synchronized void setSession(TcpClient client) {
        if (closed || (session == null ? client == null : session.client() == client)) {
            return;
        }
        long generation = registry.advanceBaselineGeneration(client != null);
        if (client != null) {
            if (serverEndpoint != null && !serverEndpoint.equals(client.serverEndpoint())) {
                registry.resetHistory();
                localIntents.values().removeIf(type -> type == FileChangeEvent.Type.CREATE);
            }
            serverEndpoint = client.serverEndpoint();
        }
        session = client == null ? null : new Session(client, Instant.now(), generation);
        worker.getQueue().clear();
        pending.clear();
        followUpModifications.clear();
        remoteChanges.clear();
        receivedChanges.clear();
        reconciliationQueued = null;
        reconciliationRunning = false;
        reconcileAgain = false;
        if (client != null) {
            requestReconciliation(client);
        }
    }

    public synchronized void onLocalChange(FileChangeEvent event) {
        if (closed || !event.ready()) {
            return;
        }
        try {
            String path = FileTransferCodec.relativePath(event.relativePath());
            if (event.type() == FileChangeEvent.Type.DELETE
                    || (event.type() == FileChangeEvent.Type.CREATE && !registry.hasBaselineRecord(path)
                        && !localIntents.containsKey(path))) {
                if (localIntents.containsKey(path) || localIntents.size() < MAX_RECONCILIATION_PATHS) {
                    localIntents.put(path, event.type());
                } else {
                    intentOverflow = true;
                }
            }
            Session current = session;
            if (current != null && event.type() != FileChangeEvent.Type.DELETE) {
                if (event.timestamp().isBefore(current.started())) {
                    requestReconciliation(current.client());
                } else {
                    queueLocal(new Job(current, path, false, false), event.type() == FileChangeEvent.Type.MODIFY, false);
                }
            }
        } catch (ProtocolException exception) {
            LOG.log(System.Logger.Level.DEBUG, "Skipping unsupported sync path");
        }
    }

    public synchronized void requestReconciliation(TcpClient client) {
        Session current = session;
        if (closed || current == null || current.client() != client || !client.isOpen()) {
            return;
        }
        if (!client.supportsReconciliation()) {
            LOG.log(System.Logger.Level.DEBUG, "Server does not support reconciliation");
            return;
        }
        if (reconciliationQueued == current) {
            reconcileAgain |= reconciliationRunning;
            return;
        }
        try {
            worker.execute(() -> reconcile(current));
            reconciliationQueued = current;
        } catch (RejectedExecutionException exception) {
            if (queueLog.allow()) {
                LOG.log(System.Logger.Level.WARNING, "Reconciliation queue is full");
            }
        }
    }

    private boolean active(Session expected) {
        return session == expected && expected.client().isOpen();
    }

    private void reconcile(Session current) {
        synchronized (this) {
            if (!active(current)) {
                return;
            }
            reconciliationRunning = true;
        }
        TcpClient client = current.client();
        BooleanSupplier cancelled = () -> !active(current) || Thread.currentThread().isInterrupted();
        int confirmed = 0;
        int deferred = 0;
        try {
            var location = new SafeRoot(root);
            var transfer = new FileTransferClient(client);
            var paths = listReconciliationPaths(current, location, transfer, cancelled);
            for (String path : paths) {
                client.lockFileOperation();
                try {
                    if (cancelled.getAsBoolean()) {
                        throw new FileTransferClient.Cancelled();
                    }
                    if (reconcilePath(current, path, location, transfer, cancelled)) {
                        confirmed++;
                    }
                } catch (IOException exception) {
                    if (cancelled.getAsBoolean()) {
                        throw exception;
                    }
                    deferred++;
                    LOG.log(System.Logger.Level.DEBUG, "Reconciliation deferred for " + path, exception);
                } finally {
                    client.unlockFileOperation();
                }
            }
            if (deferred > 0 && reconciliationLog.allow()) {
                LOG.log(System.Logger.Level.WARNING, "Reconciliation deferred {0} path(s) after I/O failures", deferred);
            }
            LOG.log(System.Logger.Level.DEBUG, "Reconciliation finished: {0} confirmed, {1} I/O failures, {2} paths checked",
                confirmed, deferred, paths.size());
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        } catch (IOException | SecurityException exception) {
            if (!cancelled.getAsBoolean()) {
                LOG.log(reconciliationFailureLog.allow() ? System.Logger.Level.ERROR : System.Logger.Level.DEBUG,
                    "Reconciliation scan failed", exception);
            }
        } finally {
            synchronized (this) {
                if (reconciliationQueued == current) {
                    reconciliationQueued = null;
                    reconciliationRunning = false;
                    if (reconcileAgain) {
                        reconcileAgain = false;
                        requestReconciliation(client);
                    }
                }
            }
            var listener = uploadCommittedListener;
            if (confirmed > 0 && active(current) && listener != null) {
                listener.accept(client);
            }
        }
    }

    private Set<String> listReconciliationPaths(Session current, SafeRoot location, FileTransferClient transfer,
                                                BooleanSupplier cancelled) throws IOException, InterruptedException {
        Set<String> paths = new LinkedHashSet<>();
        current.client().lockFileOperation();
        try {
            if (cancelled.getAsBoolean()) {
                throw new FileTransferClient.Cancelled();
            }
            for (var file : transfer.list()) {
                paths.add(file.relativePath());
            }
        } finally {
            current.client().unlockFileOperation();
        }
        synchronized (this) {
            if (cancelled.getAsBoolean()) {
                throw new FileTransferClient.Cancelled();
            }
            registry.addBaselinePaths(paths, MAX_RECONCILIATION_PATHS);
            for (String path : localIntents.keySet()) {
                addReconciliationPath(paths, path);
            }
        }
        Files.walkFileTree(location.path(), Set.of(), FileTransferCodec.MAX_PATH_COMPONENTS,
            new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult preVisitDirectory(Path directory, java.nio.file.attribute.BasicFileAttributes attrs)
                        throws IOException {
                    if (cancelled.getAsBoolean()) {
                        throw new FileTransferClient.Cancelled();
                    }
                    if (directory.equals(location.path())) {
                        return FileVisitResult.CONTINUE;
                    }
                    if (LocalFolderWatcher.ignored(directory)) {
                        return FileVisitResult.SKIP_SUBTREE;
                    }
                    try {
                        FileTransferCodec.relativePath(location.path().relativize(directory));
                        SafeRoot.directoryAttributes(directory);
                        return FileVisitResult.CONTINUE;
                    } catch (IOException exception) {
                        return FileVisitResult.SKIP_SUBTREE;
                    }
                }

                @Override
                public FileVisitResult visitFile(Path file, java.nio.file.attribute.BasicFileAttributes attrs)
                        throws IOException {
                    if (cancelled.getAsBoolean()) {
                        throw new FileTransferClient.Cancelled();
                    }
                    if (attrs.isRegularFile() && !LocalFolderWatcher.ignored(file)) {
                        String path;
                        try {
                            path = FileTransferCodec.relativePath(location.path().relativize(file));
                        } catch (ProtocolException exception) {
                            return FileVisitResult.CONTINUE;
                        }
                        addReconciliationPath(paths, path);
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        return paths;
    }

    private static void addReconciliationPath(Set<String> paths, String path) throws IOException {
        if (paths.add(path) && paths.size() > MAX_RECONCILIATION_PATHS) {
            throw new IOException("Reconciliation path limit exceeded");
        }
    }

    private boolean reconcilePath(Session current, String path, SafeRoot location, FileTransferClient transfer,
                                  BooleanSupplier cancelled) throws IOException {
        var remote = transfer.serverVersion(path).orElse(null);
        var local = readLocal(location, path, cancelled);
        String localHash = local == null ? null : local.sha256();
        String serverHash = remote == null ? null : remote.sha256();
        String base = registry.reconciliationBaseline(path).orElse(null);
        if (local != null && localHash.equals(serverHash)) {
            confirmReconciled(current, path, localHash, local, SyncFileRegistry.Origin.LOCAL);
            return true;
        }
        if (remote != null && (local == null ? canDownloadMissing(path) : localHash.equals(base))) {
            Path saved;
            try {
                saved = transfer.downloadReconciled(remote, location, local, cancelled, NO_PROGRESS,
                    publicationGuard(cancelled, local == null ? path : null));
            } catch (FileTransferClient.LocalReplacementConflict | FileAlreadyExistsException exception) {
                reportReconciliation(current, path, OPEN, null, base, serverHash,
                    "Tệp cục bộ đã thay đổi trong khi đối chiếu. Tệp được giữ nguyên.");
                return false;
            }
            confirmReconciled(current, path, serverHash, SyncFileRegistry.read(saved, cancelled),
                SyncFileRegistry.Origin.REMOTE);
            LOG.log(System.Logger.Level.INFO, "Reconciled by downloading: {0}", path);
            return true;
        }
        if (local != null && (remote == null ? canUploadCreation(path) : serverHash.equals(base))) {
            try {
                Path source = location.resolve(path, false);
                if (remote == null) {
                    transfer.upload(source, path, Optional.of(localHash),
                        () -> cancelled.getAsBoolean() || !canUploadCreation(path), NO_PROGRESS);
                } else {
                    transfer.uploadModified(source, path, base, Optional.of(localHash), cancelled, NO_PROGRESS);
                }
            } catch (FileTransferClient.Failure exception) {
                if (exception.resultCode() == FileTransferCodec.RESULT_CONFLICT) {
                    reportReconciliation(current, path, REQUIRES_RECONCILIATION, localHash, base, null,
                        "Phiên bản máy chủ đã thay đổi trong khi đối chiếu.");
                    return false;
                }
                throw exception;
            }
            confirmReconciled(current, path, localHash, local, SyncFileRegistry.Origin.LOCAL);
            LOG.log(System.Logger.Level.INFO, "Reconciled by uploading: {0}", path);
            return true;
        }
        if (local != null || remote != null || registry.hasBaselineRecord(path)) {
            boolean divergent = base != null && local != null && remote != null;
            reportReconciliation(current, path, divergent ? OPEN : REQUIRES_RECONCILIATION,
                localHash, base, serverHash, divergent ? "Cả hai phía đã thay đổi so với phiên bản đồng bộ."
                    : "Chưa đủ thông tin để đồng bộ an toàn. Các tệp hiện có được giữ nguyên.");
        }
        return false;
    }

    private static SyncFileRegistry.Fingerprint readLocal(SafeRoot location, String path, BooleanSupplier cancelled)
            throws IOException {
        try {
            return SyncFileRegistry.read(location.resolve(path, false), cancelled);
        } catch (NoSuchFileException exception) {
            return null;
        }
    }

    private synchronized boolean canDownloadMissing(String path) {
        return !intentOverflow && !localIntents.containsKey(path) && !registry.hasBaselineRecord(path);
    }

    private synchronized boolean canUploadCreation(String path) {
        return !registry.hasBaselineRecord(path) && localIntents.get(path) == FileChangeEvent.Type.CREATE;
    }

    private FileTransferClient.PublicationGuard publicationGuard(BooleanSupplier cancelled, String missingPath) {
        return publication -> {
            synchronized (this) {
                if (cancelled.getAsBoolean() || (missingPath != null && !canDownloadMissing(missingPath))) {
                    throw new FileTransferClient.Cancelled();
                }
                publication.run();
            }
        };
    }

    private synchronized void confirmReconciled(Session current, String path, String hash,
                                                SyncFileRegistry.Fingerprint fingerprint, SyncFileRegistry.Origin origin) {
        if (active(current)) {
            registry.confirmReconciled(path, hash, current.baselineGeneration());
            if (hash.equals(fingerprint.sha256())) {
                localIntents.remove(path);
                registry.remember(path, fingerprint, origin);
            }
        }
    }

    private synchronized void reportReconciliation(Session current, String path, ConflictRegistry.Status status,
                                                   String local, String base, String remote, String explanation) {
        if (active(current)) {
            if (registry.hasBaselineRecord(path)) {
                registry.invalidateBaseline(path, current.baselineGeneration());
            }
            conflicts.reportReconciliation(path, status, local, base, remote, explanation);
        }
    }

    private synchronized void queueLocal(Job job, boolean modify, boolean followUp) {
        if (closed || job.session() != session || !job.session().client().isOpen()) {
            return;
        }
        // An editor may replace a tracked path by renaming a temporary file over it.
        modify |= registry.hasBaselineRecord(job.path())
            || localIntents.get(job.path()) == FileChangeEvent.Type.DELETE;
        if (modify) {
            if (!job.session().client().supportsModifyReplacement()) {
                LOG.log(System.Logger.Level.DEBUG, "Skipping MODIFY; server capability unavailable: {0}", job.path());
                return;
            }
            if (pending.contains(job) || pending.contains(new Job(job.session(), job.path(), true, true))) {
                followUpModifications.add(job);
                return;
            }
            if (reconciliationQueued != job.session()
                    && registry.acknowledgedBaseline(job.path(), job.session().baselineGeneration()).isEmpty()) {
                LOG.log(System.Logger.Level.DEBUG, "MODIFY requires reconciliation; no valid baseline: {0}", job.path());
                requestReconciliation(job.session().client());
                return;
            }
        }
        enqueue(job, null, modify, followUp);
    }

    public void onRemoteChange(TcpClient source, FileChangeNotification notification) {
        Session current = session;
        if (current == null || source != current.client()
                || !FileTransferCodec.validRelativePath(notification.relativePath())) {
            return;
        }
        if (notification.type() == FileChangeNotification.Type.MODIFY) {
            queueRemoteModification(new Job(current, notification.relativePath(), true, true), notification);
        } else if (notification.type() == FileChangeNotification.Type.CREATE) {
            enqueue(new Job(current, notification.relativePath(), true, false), notification, false, false);
        }
    }

    private synchronized void queueRemoteModification(Job job, FileChangeNotification event) {
        if (closed || job.session() != session || !job.session().client().isOpen()
                || !job.session().client().supportsModifyReplacement()
                || event.originSessionId().equals(FileTransferCodec.NONE)
                || (!job.session().client().supportsRestore()
                    && event.originSessionId().toString().equals(job.session().client().debugSessionId()))
                || event.eventId().equals(FileTransferCodec.NONE) || !receivedChanges.add(event.eventId())) {
            return;
        }
        if (receivedChanges.size() > SEEN_CHANGES_LIMIT) {
            receivedChanges.removeFirst();
        }
        String previous = event.previousSha256().orElse("");
        String next = event.sha256().orElse("");
        if (!previous.matches("[0-9a-f]{64}") || !next.matches("[0-9a-f]{64}")) {
            rejectRemoteChain(job, event, "Thông báo cập nhật thiếu hoặc có mã SHA-256 không hợp lệ.");
            return;
        }
        if (previous.equals(next)) {
            // A restore can create a history entry without changing synchronized bytes.
            return;
        }

        var chain = remoteChanges.get(job);
        if (chain != null && (chain.size() >= REMOTE_CHAIN_LIMIT
                || !chain.getLast().sha256().equals(event.previousSha256()))) {
            if (!deferRemoteChain(job)) {
                rejectRemoteChain(job, event, "Chuỗi phiên bản bị gián đoạn hoặc thông báo đến sai thứ tự.");
            }
            return;
        }
        if (chain == null && remoteChanges.size() >= QUEUE_LIMIT) {
            var oldestInactive = remoteChanges.keySet().stream().filter(key -> !pending.contains(key)).findFirst();
            if (oldestInactive.isEmpty()) {
                rejectRemoteChain(job, event, "Đã vượt giới hạn theo dõi chuỗi cập nhật.");
                return;
            }
            Job evicted = oldestInactive.get();
            rejectRemoteChain(evicted, remoteChanges.get(evicted).getLast(), "Không còn đủ thông báo để kiểm tra chuỗi cập nhật.");
        }
        remoteChanges.computeIfAbsent(job, key -> new ArrayList<>()).add(event);
        if (!pending.contains(job)) {
            enqueue(job, null, false, false);
            if (!pending.contains(job)) {
                if (!deferRemoteChain(job)) {
                    rejectRemoteChain(job, event, "Hàng đợi đầy. Cần đối chiếu phiên bản với máy chủ.");
                }
            }
        }
    }

    private synchronized boolean deferRemoteChain(Job job) {
        if (!active(job.session()) || !job.session().client().supportsReconciliation()) {
            return false;
        }
        remoteChanges.remove(job);
        requestReconciliation(job.session().client());
        return true;
    }

    private void rejectRemoteChain(Job job, FileChangeNotification event, String reason) {
        remoteChanges.remove(job);
        String anchor = registry.remoteAnchor(job.path(), job.session().baselineGeneration()).orElse(null);
        registry.invalidateBaseline(job.path(), job.session().baselineGeneration());
        conflicts.report(job.path(), NOTIFICATION_CHAIN, REQUIRES_RECONCILIATION,
            null, anchor, event.sha256().orElse(null), reason);
        requestReconciliation(job.session().client());
    }

    private synchronized List<FileChangeNotification> remoteSnapshot(Job job) {
        return List.copyOf(remoteChanges.getOrDefault(job, List.of()));
    }

    private synchronized boolean isLatest(Job job, FileChangeNotification expected) {
        var chain = remoteChanges.get(job);
        return chain != null && chain.getLast().eventId().equals(expected.eventId());
    }

    private synchronized void applied(Job job, FileChangeNotification expected, String anchor,
                                      String hash, SyncFileRegistry.Fingerprint fingerprint) {
        if (!active(job.session())) {
            return;
        }
        if (fingerprint != null) {
            registry.remember(job.path(), fingerprint, SyncFileRegistry.Origin.REMOTE);
        }
        var chain = remoteChanges.get(job);
        if (chain == null) {
            return;
        }
        for (int index = 0; index < chain.size(); index++) {
            if (chain.get(index).eventId().equals(expected.eventId())) {
                registry.acknowledgeRemote(job.path(), anchor, hash, job.session().baselineGeneration());
                chain.subList(0, index + 1).clear();
                if (chain.isEmpty()) {
                    remoteChanges.remove(job);
                }
                return;
            }
        }
    }

    private synchronized void enqueue(Job job, FileChangeNotification notification, boolean modify, boolean followUp) {
        if (closed || job.session() != session || !job.session().client().isOpen() || !pending.add(job)) {
            return;
        }
        try {
            // Reserve one slot for the running job's coalesced follow-up.
            if (!followUp && worker.getQueue().remainingCapacity() <= 1) {
                throw new RejectedExecutionException("Sync queue is at its admission limit");
            }
            worker.execute(() -> execute(job, notification, modify));
        } catch (RejectedExecutionException exception) {
            pending.remove(job);
            if (queueLog.allow()) {
                LOG.log(System.Logger.Level.WARNING, "Sync queue full; work skipped");
            }
            requestReconciliation(job.session().client());
        }
    }

    private void execute(Job job, FileChangeNotification notification, boolean modify) {
        if (!job.download()) {
            synchronized (this) {
                modify |= registry.hasBaselineRecord(job.path())
                    || localIntents.get(job.path()) == FileChangeEvent.Type.DELETE;
            }
        }
        TcpClient client = job.session().client();
        var changes = job.remoteModify() ? remoteSnapshot(job) : List.<FileChangeNotification>of();
        var latest = changes.isEmpty() ? null : changes.getLast();
        BooleanSupplier cancelled = () -> session != job.session() || !client.isOpen()
            || Thread.currentThread().isInterrupted()
            || (job.remoteModify() && (latest == null || !isLatest(job, latest)));
        try {
            for (int attempt = 1; attempt <= MAX_ATTEMPTS && !cancelled.getAsBoolean(); attempt++) {
                try {
                    boolean committedChange = false;
                    client.lockFileOperation();
                    try {
                        if (cancelled.getAsBoolean()) {
                            return;
                        }
                        SafeRoot location = new SafeRoot(root);
                        var transfer = new FileTransferClient(client);
                        if (job.remoteModify()) {
                            committedChange = downloadModification(location, job, changes, transfer, cancelled);
                        } else if (job.download()) {
                            download(location, job, notification, transfer, cancelled);
                        } else {
                            committedChange = upload(location, job, transfer, cancelled, modify);
                        }
                    } finally {
                        client.unlockFileOperation();
                    }
                    var listener = job.remoteModify() ? remoteModifyAppliedListener : uploadCommittedListener;
                    if (committedChange && active(job.session()) && listener != null) {
                        listener.accept(client);
                    }
                    return;
                } catch (FileTransferClient.Cancelled exception) {
                    return;
                } catch (FileTransferClient.Failure exception) {
                    if (!modify && exception.resultCode() == FileTransferCodec.RESULT_CONFLICT) {
                        requestReconciliation(client);
                    }
                    if (modify && exception.resultCode() == FileTransferCodec.RESULT_CONFLICT) {
                        synchronized (this) {
                            if (active(job.session())) {
                                registry.invalidateBaseline(job.path(), job.session().baselineGeneration());
                                requestReconciliation(client);
                            }
                        }
                    } else {
                        LOG.log(exception.resultCode() == FileTransferCodec.RESULT_CONFLICT
                            ? System.Logger.Level.DEBUG : System.Logger.Level.ERROR, "Sync failed: " + job.path(), exception);
                    }
                    return;
                } catch (IOException exception) {
                    if (cancelled.getAsBoolean()) {
                        return;
                    }
                    if (attempt == MAX_ATTEMPTS || !transientFailure(exception)) {
                        LOG.log(System.Logger.Level.ERROR, "Sync failed: " + job.path(), exception);
                        return;
                    }
                    Thread.sleep(250L * attempt);
                }
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        } catch (SecurityException exception) {
            LOG.log(System.Logger.Level.ERROR, "Sync access denied: " + job.path(), exception);
        } finally {
            synchronized (this) {
                pending.remove(job);
                if (job.remoteModify()) {
                    var chain = remoteChanges.get(job);
                    if (job.session() == session && chain != null
                            && (latest == null || !chain.getLast().eventId().equals(latest.eventId()))) {
                        enqueue(job, null, false, true);
                    }
                    Job local = new Job(job.session(), job.path(), false, false);
                    if (!pending.contains(job) && followUpModifications.remove(local)) {
                        queueLocal(local, true, true);
                    }
                } else if (followUpModifications.remove(job)) {
                    queueLocal(job, true, true);
                }
            }
        }
    }

    private static boolean transientFailure(IOException exception) {
        return exception instanceof SocketTimeoutException || exception instanceof SocketException
            || exception instanceof AccessDeniedException;
    }

    private boolean upload(SafeRoot location, Job job, FileTransferClient transfer,
                            BooleanSupplier cancelled, boolean modify) throws IOException {
        String path = job.path();
        Path source = location.resolve(path, false);
        var fingerprint = SyncFileRegistry.read(source, cancelled);
        if (registry.contains(path, fingerprint)) {
            return false;
        }
        if (modify) {
            var base = registry.acknowledgedBaseline(path, job.session().baselineGeneration());
            if (base.isEmpty()) {
                LOG.log(System.Logger.Level.DEBUG, "MODIFY requires reconciliation; no valid baseline: {0}", path);
                return false;
            }
            if (base.get().equals(fingerprint.sha256())) {
                return false;
            }
            try {
                transfer.uploadModified(location.resolve(path, false), path, base.get(),
                    Optional.of(fingerprint.sha256()), cancelled, NO_PROGRESS);
            } catch (FileTransferClient.Failure exception) {
                if (exception.resultCode() == FileTransferCodec.RESULT_CONFLICT) {
                    synchronized (this) {
                        if (active(job.session())) {
                            conflicts.report(path, SERVER_BASE_MISMATCH, OPEN, fingerprint.sha256(), base.get(), null,
                                "Máy chủ đã thay đổi hoặc không còn bản gốc. Tệp cục bộ được giữ nguyên.");
                        }
                    }
                }
                throw exception;
            }
        } else {
                transfer.upload(location.resolve(path, false), path, Optional.of(fingerprint.sha256()), cancelled, NO_PROGRESS);
        }
        rememberTransfer(job, fingerprint, SyncFileRegistry.Origin.LOCAL, true);
        LOG.log(System.Logger.Level.INFO, "Uploaded {0} ({1})", path, modify ? "MODIFY" : "CREATE");
        return true;
    }

    private boolean downloadModification(SafeRoot location, Job job, List<FileChangeNotification> changes,
                                         FileTransferClient transfer, BooleanSupplier cancelled) throws IOException {
        var latest = changes.getLast();
        String path = job.path();
        long generation = job.session().baselineGeneration();
        String targetHash = latest.sha256().orElseThrow();
        var acknowledged = job.session().client().supportsReconciliation()
            ? registry.acknowledgedBaseline(path, generation) : registry.remoteAnchor(path, generation);
        if (acknowledged.isEmpty()) {
            if (!deferRemoteChain(job)) {
                reportRemoteConflict(job, latest, NOTIFICATION_CHAIN, REQUIRES_RECONCILIATION,
                    null, null, targetHash, "Chưa có mã SHA-256 của phiên bản đã đồng bộ trong phiên hiện tại.");
            }
            LOG.log(System.Logger.Level.DEBUG, "Remote MODIFY requires reconciliation; unknown baseline: {0}", path);
            return false;
        }
        String anchor = acknowledged.get();
        if (!anchor.equals(targetHash)
                && changes.stream().noneMatch(event -> event.previousSha256().filter(anchor::equals).isPresent())) {
            if (!deferRemoteChain(job)) {
                reportRemoteConflict(job, latest, NOTIFICATION_CHAIN, REQUIRES_RECONCILIATION,
                    null, anchor, targetHash, "Không thể nối chuỗi cập nhật với phiên bản đã đồng bộ.");
            }
            LOG.log(System.Logger.Level.DEBUG, "Remote MODIFY notification gap: {0}", path);
            return false;
        }
        SyncFileRegistry.Fingerprint fingerprint;
        try {
            fingerprint = SyncFileRegistry.read(location.resolve(path, false), cancelled);
        } catch (NoSuchFileException | SyncFileRegistry.FileChangedException exception) {
            if (!anchor.equals(targetHash)) {
                reportRemoteConflict(job, latest, LOCAL_EDITS, OPEN, null, anchor, targetHash,
                    "Tệp cục bộ đã thay đổi hoặc không còn tồn tại. Không áp dụng bản máy chủ.");
            }
            return false;
        }
        if (fingerprint.sha256().equals(targetHash)) {
            applied(job, latest, anchor, targetHash, null);
            return false;
        }
        if (anchor.equals(targetHash)) {
            // A replay of an applied version must not invalidate a newer unsent local edit.
            return false;
        }
        if (!fingerprint.sha256().equals(anchor)) {
            reportRemoteConflict(job, latest, LOCAL_EDITS, OPEN, fingerprint.sha256(), anchor, targetHash,
                "Tệp cục bộ có nội dung khác bản gốc. Các thay đổi cục bộ được giữ nguyên.");
            return false;
        }
        FileMetadata metadata = latest.metadata().orElseGet(
            () -> new FileMetadata(path, 0, latest.timestamp().toEpochMilli()));
        Path result;
        try {
            result = transfer.downloadModified(metadata, location, targetHash, fingerprint,
                latest.metadata().isPresent(), cancelled, NO_PROGRESS, publicationGuard(cancelled, null));
        } catch (FileTransferClient.LocalReplacementConflict exception) {
            String currentHash = null;
            try {
                currentHash = SyncFileRegistry.read(location.resolve(path, false), cancelled).sha256();
            } catch (IOException ignored) {
                // The local file may still be changing or may have been removed.
            }
            reportRemoteConflict(job, latest, LOCAL_CHANGED_DURING_DOWNLOAD, OPEN, currentHash, anchor, targetHash,
                "Tệp cục bộ đã thay đổi trong khi tải xuống. Không thay thế tệp.");
            throw exception;
        } catch (FileTransferClient.Failure exception) {
            if (exception.resultCode() == FileTransferCodec.RESULT_CONFLICT && !deferRemoteChain(job)) {
                reportRemoteConflict(job, latest, NOTIFICATION_CHAIN, REQUIRES_RECONCILIATION, fingerprint.sha256(),
                    anchor, null, "Thông tin tệp máy chủ đã thay đổi sau khi gửi thông báo.");
            }
            throw exception;
        }
        var downloaded = SyncFileRegistry.read(result, cancelled);
        applied(job, latest, anchor, targetHash, targetHash.equals(downloaded.sha256()) ? downloaded : null);
        LOG.log(System.Logger.Level.INFO, "Downloaded {0} (MODIFY)", path);
        return true;
    }

    private synchronized void reportRemoteConflict(Job job, FileChangeNotification expected, ConflictRegistry.Type type,
                                                     ConflictRegistry.Status status, String local, String base,
                                                     String remote, String explanation) {
        if (active(job.session()) && isLatest(job, expected)) {
            registry.invalidateBaseline(job.path(), job.session().baselineGeneration());
            conflicts.report(job.path(), type, status, local, base, remote, explanation);
        }
    }

    private void download(SafeRoot location, Job job, FileChangeNotification notification,
                          FileTransferClient transfer, BooleanSupplier cancelled) throws IOException {
        String path = job.path();
        Optional<String> hash = notification.sha256();
        var local = readLocal(location, path, cancelled);
        if (local != null) {
            if (hash.filter(local.sha256()::equals).isEmpty() || !registry.hasBaselineRecord(path)) {
                requestReconciliation(job.session().client());
            }
            return;
        }
        if (!canDownloadMissing(path)) {
            requestReconciliation(job.session().client());
            return;
        }
        FileMetadata file = notification.metadata().orElseGet(
            () -> new FileMetadata(path, 0, notification.timestamp().toEpochMilli()));
        Path saved;
        try {
            saved = transfer.downloadToRoot(file, location, hash, cancelled, NO_PROGRESS, publicationGuard(cancelled, path));
        } catch (FileAlreadyExistsException exception) {
            requestReconciliation(job.session().client());
            return;
        }
        var fingerprint = SyncFileRegistry.read(saved, cancelled);
        if (hash.isEmpty() || hash.get().equals(fingerprint.sha256())) {
            rememberTransfer(job, fingerprint, SyncFileRegistry.Origin.REMOTE, hash.isPresent());
        } else {
            synchronized (this) {
                if (active(job.session())) {
                    registry.recordConfirmedBaseline(path, hash.get(), job.session().baselineGeneration());
                }
            }
        }
        LOG.log(System.Logger.Level.INFO, "Downloaded {0} (CREATE)", path);
    }

    private synchronized void rememberTransfer(Job job, SyncFileRegistry.Fingerprint fingerprint,
                                               SyncFileRegistry.Origin origin, boolean confirmed) {
        if (active(job.session())) {
            if (confirmed) {
                registry.recordConfirmedBaseline(job.path(), fingerprint.sha256(), job.session().baselineGeneration());
            }
            if (localIntents.get(job.path()) == FileChangeEvent.Type.CREATE) {
                localIntents.remove(job.path());
            }
            registry.remember(job.path(), fingerprint, origin);
        }
    }

    @Override
    public synchronized void close() {
        closed = true;
        uploadCommittedListener = null;
        remoteModifyAppliedListener = null;
        registry.advanceBaselineGeneration(false);
        session = null;
        pending.clear();
        followUpModifications.clear();
        remoteChanges.clear();
        receivedChanges.clear();
        worker.shutdownNow();
    }
}
