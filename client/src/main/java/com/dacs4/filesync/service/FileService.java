package com.dacs4.filesync.service;

import com.dacs4.filesync.network.FileTransferClient;
import com.dacs4.filesync.network.TcpClient;
import com.dacs4.filesync.shared.dto.FileMetadata;
import javafx.beans.property.ReadOnlyBooleanProperty;
import javafx.beans.property.ReadOnlyBooleanWrapper;
import javafx.beans.property.ReadOnlyDoubleProperty;
import javafx.beans.property.ReadOnlyDoubleWrapper;
import javafx.concurrent.Task;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/** One file operation at a time. Public methods are called from the JavaFX thread. */
public final class FileService {
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
                return operation.run(new FileTransferClient(session), token,
                    (bytes, total) -> updateProgress(total == 0 ? 1 : bytes, total == 0 ? 1 : total));
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
