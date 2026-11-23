package com.dacs4.filesync.service;

import com.dacs4.filesync.network.FileTransferClient;
import com.dacs4.filesync.network.TcpClient;
import com.dacs4.filesync.shared.dto.FileMetadata;
import com.dacs4.filesync.shared.dto.FileVersion;
import com.dacs4.filesync.shared.protocol.FileTransferCodec;
import javafx.beans.property.ReadOnlyBooleanProperty;
import javafx.beans.property.ReadOnlyBooleanWrapper;
import javafx.beans.property.ReadOnlyDoubleProperty;
import javafx.beans.property.ReadOnlyDoubleWrapper;
import javafx.concurrent.Task;

import java.nio.file.Path;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.BooleanSupplier;

/** One file operation at a time. Public methods are called from the JavaFX thread. */
public final class FileService {
    private static final System.Logger RESTORE_LOG = System.getLogger("com.dacs4.filesync.log.Restore");
    private final ConnectionService connection;
    private final ReadOnlyBooleanWrapper busy = new ReadOnlyBooleanWrapper();
    private final ReadOnlyBooleanWrapper cancellable = new ReadOnlyBooleanWrapper();
    private final ReadOnlyDoubleWrapper progress = new ReadOnlyDoubleWrapper(-1);
    private AtomicBoolean cancellation;

    public FileService(ConnectionService connection) {
        this.connection = connection;
    }

    public ReadOnlyBooleanProperty busyProperty() { return busy.getReadOnlyProperty(); }
    public ReadOnlyBooleanProperty cancellableProperty() { return cancellable.getReadOnlyProperty(); }
    public ReadOnlyDoubleProperty progressProperty() { return progress.getReadOnlyProperty(); }

    public void list(Consumer<List<FileMetadata>> success, Consumer<Throwable> failure) {
        start(false, (client, cancelled, report) -> client.list(), success, failure);
    }

    /** Fetches history on the file worker and delivers callbacks on the JavaFX thread. */
    public void listVersions(String path, Consumer<List<FileVersion>> success, Consumer<Throwable> failure) {
        startSessionOperation((client, cancelled, report) -> client.listVersions(path), success, failure);
    }

    public void readRestoreBase(String path, Consumer<String> success, Consumer<Throwable> failure) {
        startSessionOperation((client, cancelled, report) -> client.serverVersion(path)
            .orElseThrow(() -> new IOException("Tệp hiện tại không còn trên máy chủ.")).sha256(), success, error -> {
                RESTORE_LOG.log(System.Logger.Level.ERROR, "Could not verify current version for restore" + pathContext(path), error);
                failure.accept(error);
            });
    }

    public void compare(Path localRoot, String path, BooleanSupplier cancelled,
                        Consumer<ConflictComparison.Result> success, Consumer<Throwable> failure) {
        startSessionOperation((client, token, report) ->
            ConflictComparison.capture(client, localRoot, path, () -> token.get() || cancelled.getAsBoolean(), report),
            success, failure);
    }

    public void restoreVersion(String path, UUID versionId, String expectedHash, Runnable success,
                               Consumer<Throwable> failure) {
        startSessionOperation((client, cancelled, report) -> {
            client.restoreVersion(path, versionId, expectedHash);
            return null;
        }, ignored -> {
            RESTORE_LOG.log(System.Logger.Level.INFO, "Client confirmed restore" + pathContext(path));
            success.run();
        }, error -> {
            boolean conflict = error instanceof FileTransferClient.Failure rejected
                && rejected.resultCode() == FileTransferCodec.RESULT_CONFLICT;
            RESTORE_LOG.log(conflict ? System.Logger.Level.WARNING : System.Logger.Level.ERROR,
                (conflict ? "Client restore rejected; current version changed" : "Client restore could not be confirmed")
                    + pathContext(path), error);
            failure.accept(error);
        });
    }

    private static String pathContext(String path) {
        return FileTransferCodec.validRelativePath(path) ? ": " + path : "";
    }

    private <T> void startSessionOperation(Operation<T> operation, Consumer<T> success, Consumer<Throwable> failure) {
        TcpClient session = connection.connectedClient();
        if (session == null || !session.isOpen()) {
            failure.accept(new IOException("Phiên kết nối không còn hoạt động."));
            return;
        }
        if (busy.get()) {
            failure.accept(new IllegalStateException("Đang thực hiện thao tác tệp khác."));
            return;
        }
        start(false, operation, result -> {
            if (connection.connectedClient() == session && session.isOpen()) {
                success.accept(result);
            } else {
                failure.accept(new IOException("Phiên kết nối đã thay đổi."));
            }
        }, failure);
    }

    public void upload(Path path, Runnable success, Consumer<Throwable> failure) {
        start(true, (client, cancelled, report) -> {
            client.upload(path, cancelled::get, report);
            return null;
        }, ignored -> success.run(), failure);
    }

    public void download(FileMetadata file, Consumer<Path> success, Consumer<Throwable> failure) {
        start(true, (client, cancelled, report) -> {
            String configured = System.getProperty("filesync.download.dir");
            Path directory = configured == null
                ? Path.of(System.getProperty("user.home"), "Downloads", "DistributedFileSync")
                : Path.of(configured);
            return client.download(file, directory, cancelled::get, report);
        }, success, failure);
    }

    public void cancel() {
        if (cancellable.get()) {
            cancellation.set(true);
            cancellable.set(false);
        }
    }

    private <T> void start(boolean transfer, Operation<T> operation, Consumer<T> success,
                           Consumer<Throwable> failure) {
        TcpClient session = connection.connectedClient();
        if (busy.get() || session == null) {
            return;
        }
        AtomicBoolean token = new AtomicBoolean();
        cancellation = token;
        Task<T> task = new Task<>() {
            @Override
            protected T call() throws Exception {
                session.lockFileOperation();
                try {
                    if (token.get()) {
                        throw new FileTransferClient.Cancelled();
                    }
                    return operation.run(new FileTransferClient(session), token,
                        (bytes, total) -> updateProgress(total == 0 ? 1 : bytes, total == 0 ? 1 : total));
                } finally {
                    session.unlockFileOperation();
                }
            }
        };
        busy.set(true);
        cancellable.set(transfer);
        progress.bind(task.progressProperty());
        task.setOnSucceeded(event -> {
            finish();
            success.accept(task.getValue());
        });
        task.setOnFailed(event -> {
            finish();
            failure.accept(task.getException());
        });
        // Finish temporary-file cleanup even if JavaFX exits during the operation.
        Thread.ofPlatform().name("file-operation").start(task);
    }

    private void finish() {
        progress.unbind();
        busy.set(false);
        cancellable.set(false);
        cancellation = null;
    }

    @FunctionalInterface
    private interface Operation<T> {
        T run(FileTransferClient client, AtomicBoolean cancelled, FileTransferClient.Progress progress)
            throws Exception;
    }
}
