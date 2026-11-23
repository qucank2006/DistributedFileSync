package com.dacs4.filesync.sync;

import com.dacs4.filesync.Main;
import com.dacs4.filesync.controller.ShellController;
import com.dacs4.filesync.network.TcpClient;
import com.dacs4.filesync.network.FileTransferClient;
import com.dacs4.filesync.service.ConnectionService;
import com.dacs4.filesync.service.FileService;
import com.dacs4.filesync.shared.dto.FileVersion;
import com.dacs4.filesync.server.config.ServerConfig;
import com.dacs4.filesync.server.network.TcpServer;
import com.dacs4.filesync.shared.dto.FileMetadata;
import com.dacs4.filesync.shared.protocol.MessageCodec;
import com.dacs4.filesync.shared.protocol.MessageType;
import com.dacs4.filesync.shared.protocol.ProtocolMessage;
import com.dacs4.filesync.shared.protocol.FileTransferCodec;
import javafx.application.Platform;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.ComboBox;
import javafx.scene.control.TableView;
import javafx.scene.control.TextField;
import javafx.stage.Stage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.OptionalLong;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.dacs4.filesync.sync.FileChangeEvent.Type.*;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(45)
class ReconciliationTest {
    @Test
    void comparisonCapturesRealServerBytesWithoutUpdatingLocalFile() throws Exception {
        Files.writeString(local.resolve("example.java"), "class Example {\n  int old;\n}\n");
        upload("example.java", "class Example {\n  int working;\n}\n");
        var result = compare("example.java", () -> false, (bytes, total) -> {});
        assertEquals(digest("class Example {\n  int old;\n}\n"), result.localSha256());
        assertEquals(digest("class Example {\n  int working;\n}\n"), result.serverSha256());
        assertEquals(java.util.List.of(1), result.diff().differences());
        assertTrue(hasContent(local, "example.java", "class Example {\n  int old;\n}\n"));
    }

    @Test
    void comparisonNeverCreatesAMissingLocalRoot() throws Exception {
        Path missing = local.resolve("missing-root");
        var failure = assertThrows(IOException.class, () -> com.dacs4.filesync.service.ConflictComparison.capture(
            new FileTransferClient(peer), missing, "example.txt", () -> false, (bytes, total) -> {}));
        assertTrue(failure.getMessage().contains("Không tìm thấy tệp cục bộ"));
        assertFalse(Files.exists(missing));
    }

    @Test
    void comparisonAbortsWhenLocalChangesDuringDownload() throws Exception {
        Files.writeString(local.resolve("changed.txt"), "local");
        upload("changed.txt", "server");
        var failure = assertThrows(IOException.class, () -> compare("changed.txt", () -> false, (bytes, total) -> {
            if (bytes == 0) {
                try {
                    Files.writeString(local.resolve("changed.txt"), "edit during comparison");
                } catch (IOException exception) {
                    throw new java.io.UncheckedIOException(exception);
                }
            }
        }));
        assertTrue(failure.getMessage().contains("cục bộ đã thay đổi"), failure.getMessage());
        assertTrue(hasContent(local, "changed.txt", "edit during comparison"));
    }

    @Test
    void comparisonAbortsWhenServerChangesDuringDownload() throws Exception {
        Files.writeString(local.resolve("changed.txt"), "local");
        upload("changed.txt", "server");
        try (var reader = new TcpClient()) {
            reader.connect("127.0.0.1", port, "comparison-reader", event -> {});
            reader.lockFileOperation();
            try {
                var failure = assertThrows(IOException.class, () -> com.dacs4.filesync.service.ConflictComparison.capture(
                    new FileTransferClient(reader), local, "changed.txt", () -> false, (bytes, total) -> {
                        if (bytes == 0) {
                            try {
                                Files.writeString(storage.resolve("changed.txt"), "change");
                            } catch (IOException exception) {
                                throw new java.io.UncheckedIOException(exception);
                            }
                        }
                    }));
                assertTrue(failure.getMessage().contains("máy chủ đã thay đổi"), failure.getMessage());
                assertTrue(hasContent(storage, "changed.txt", "change"));
            } finally {
                reader.unlockFileOperation();
            }
        }
    }

    @Test
    void comparisonCancellationDuringDownloadLeavesSessionUsable() throws Exception {
        Files.writeString(local.resolve("example.txt"), "local");
        upload("example.txt", "server");
        var cancelled = new AtomicBoolean();
        assertThrows(FileTransferClient.Cancelled.class, () ->
            compare("example.txt", cancelled::get, (bytes, total) -> cancelled.set(true)));
        assertTrue(hasContent(local, "example.txt", "local"));
        peer.ping();
        assertEquals(java.util.List.of(0), compare("example.txt", () -> false, (bytes, total) -> {}).diff().differences());
    }

    @Test
    void comparisonDisconnectDuringDownloadLeavesLocalUnchanged() throws Exception {
        Files.writeString(local.resolve("example.txt"), "local");
        upload("example.txt", "server");
        assertThrows(IOException.class, () -> compare("example.txt", () -> false, (bytes, total) -> {
            try {
                peer.close();
            } catch (IOException exception) {
                throw new java.io.UncheckedIOException(exception);
            }
        }));
        assertTrue(hasContent(local, "example.txt", "local"));
    }

    @Test
    void comparisonRejectsOversizedRemoteFileBeforeDownloading() throws Exception {
        Files.writeString(local.resolve("example.txt"), "local");
        upload("example.txt", "a".repeat(com.dacs4.filesync.service.LineDiff.MAX_BYTES + 1));
        assertTrue(assertThrows(IOException.class, () ->
            compare("example.txt", () -> false, (bytes, total) -> fail("Download should not start")))
            .getMessage().contains("512 KiB"));
    }

    private com.dacs4.filesync.service.ConflictComparison.Result compare(String path,
            java.util.function.BooleanSupplier cancelled, FileTransferClient.Progress progress) throws Exception {
        peer.lockFileOperation();
        try {
            return com.dacs4.filesync.service.ConflictComparison.capture(
                new FileTransferClient(peer), local, path, cancelled, progress);
        } finally {
            peer.unlockFileOperation();
        }
    }
    @TempDir Path temporary;
    private Path storage;
    private Path local;
    private Path manual;
    private TcpServer server;
    private TcpClient client;
    private TcpClient peer;
    private SyncTransferManager manager;
    private final AtomicInteger confirmations = new AtomicInteger();
    private final AtomicBoolean dropNextNotification = new AtomicBoolean();
    private int port;

    @BeforeEach
    void start() throws Exception {
        storage = Files.createDirectory(temporary.resolve("storage"));
        local = Files.createDirectory(temporary.resolve("local"));
        manual = Files.createDirectory(temporary.resolve("manual"));
        startServer();
        createManager();
    }

    private void startServer() throws IOException {
        try (var portFinder = new ServerSocket(0)) {
            port = portFinder.getLocalPort();
        }
        server = new TcpServer(new ServerConfig("127.0.0.1", port, storage, 128L * 1024 * 1024));
        Thread.ofVirtual().start(() -> {
            try {
                server.serve();
            } catch (IOException exception) {
                throw new AssertionError(exception);
            }
        });
        peer = new TcpClient();
        peer.connect("127.0.0.1", port, "test-peer", event -> {});
    }

    @AfterEach
    void stop() throws Exception {
        disconnect();
        manager.close();
        peer.close();
        server.close();
    }

    @Test
    void reconnectDownloadsCreateMissedWhileOffline() throws Exception {
        connect();
        disconnect();
        upload("nested/new.txt", "missed CREATE");
        connect();
        await(() -> hasContent(local, "nested/new.txt", "missed CREATE"));
        assertTrue(manager.conflicts().snapshot().isEmpty());
    }

    @Test
    void reconnectDownloadsServerModifyFromAcknowledgedBaseline() throws Exception {
        establish("file.txt", "base");
        disconnect();
        modify("file.txt", "server edit");
        connect();
        await(() -> hasContent(local, "file.txt", "server edit"));
        assertTrue(manager.conflicts().snapshot().isEmpty());
    }

    @Test
    void versionStorageSurvivesLocalhostServerRestartAndStaysOutOfProtocolListings() throws Exception {
        establish("file.txt", "base");
        modify("file.txt", "second");
        await(() -> hasContent(local, "file.txt", "second"));
        disconnect();
        peer.close();
        server.close();
        startServer();
        peer.ping();
        var transfer = new FileTransferClient(peer);
        peer.lockFileOperation();
        try {
            assertEquals(java.util.List.of("file.txt"), transfer.list().stream().map(FileMetadata::name).toList());
            assertTrue(peer.supportsVersionHistory());
            var version = transfer.listVersions("file.txt").getFirst();
            assertEquals("file.txt", version.relativePath());
            assertEquals(4, version.size());
            assertEquals(java.util.HexFormat.of().formatHex(FileTransferCodec.sha256()
                .digest("base".getBytes(java.nio.charset.StandardCharsets.UTF_8))), version.sha256());
            assertTrue(transfer.listVersions("missing.txt").isEmpty());
        } finally {
            peer.unlockFileOperation();
        }
        modify("file.txt", "third");
        Path versions = temporary.resolve(".filesync-versions-storage");
        try (var entries = Files.list(versions)) {
            var contents = new java.util.ArrayList<String>();
            for (Path entry : entries.filter(Files::isDirectory).toList()) {
                contents.add(Files.readString(entry.resolve("content")));
            }
            contents.sort(String::compareTo);
            assertEquals(java.util.List.of("base", "second"), contents);
        }
        assertTrue(hasContent(storage, "file.txt", "third"));
    }

    @Test
    void clientSkipsHistoryRequestsOnOlderServersAndDisconnectedSessions() throws Exception {
        try (var legacy = new ServerSocket(0);
             var worker = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();
             var connection = new TcpClient()) {
            var exchange = worker.submit(() -> {
                try (var socket = legacy.accept()) {
                    socket.setSoTimeout(5000);
                    var input = new java.io.DataInputStream(socket.getInputStream());
                    var output = new java.io.DataOutputStream(socket.getOutputStream());
                    var hello = MessageCodec.read(input);
                    assertEquals(MessageType.HELLO, hello.type());
                    MessageCodec.write(output, new ProtocolMessage(MessageType.WELCOME,
                        java.util.UUID.randomUUID() + FileTransferCodec.MODIFY_REPLACEMENT_CAPABILITY
                            + FileTransferCodec.CONDITIONAL_MODIFY_CAPABILITY));
                    assertEquals(MessageType.PING, MessageCodec.read(input).type());
                    MessageCodec.write(output, new ProtocolMessage(MessageType.PONG));
                }
                return null;
            });
            var transfer = new FileTransferClient(connection);
            assertThrows(IOException.class, () -> transfer.listVersions("file.txt"));
            connection.connect("127.0.0.1", legacy.getLocalPort(), "legacy-test");
            assertFalse(connection.supportsVersionHistory());
            var unsupported = assertThrows(FileTransferClient.Failure.class, () -> transfer.listVersions("file.txt"));
            assertEquals(FileTransferCodec.RESULT_NOT_IMPLEMENTED, unsupported.resultCode());
            assertFalse(connection.supportsRestore());
            var restoreUnsupported = assertThrows(FileTransferClient.Failure.class,
                () -> transfer.restoreVersion("file.txt", java.util.UUID.randomUUID(), digest("base")));
            assertEquals(FileTransferCodec.RESULT_NOT_IMPLEMENTED, restoreUnsupported.resultCode());
            connection.ping();
            exchange.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void missingLiveNotificationTriggersReconciliationWhenNextModifyArrives() throws Exception {
        establish("file.txt", "base");
        dropNextNotification.set(true);
        modify("file.txt", "missed intermediate version");
        await(() -> !dropNextNotification.get());
        modify("file.txt", "latest version");
        await(() -> hasContent(local, "file.txt", "latest version"));
        assertTrue(manager.conflicts().snapshot().isEmpty());
    }

    @Test
    void reconnectUploadsLocalOfflineEditConditionally() throws Exception {
        establish("file.txt", "base");
        disconnect();
        Files.writeString(local.resolve("file.txt"), "offline edit");
        event(MODIFY, "file.txt");
        connect();
        await(() -> hasContent(storage, "file.txt", "offline edit"));
        assertTrue(manager.conflicts().snapshot().isEmpty());
    }

    @Test
    void offlineDivergencePreservesBothVersionsAndConflictId() throws Exception {
        establish("file.txt", "base");
        disconnect();
        Files.writeString(local.resolve("file.txt"), "local edit");
        modify("file.txt", "remote edit");
        connect();
        await(() -> conflict("file.txt") != null);
        var first = conflict("file.txt");
        assertEquals(ConflictRegistry.Status.OPEN, first.status());
        assertTrue(hasContent(local, "file.txt", "local edit"));
        assertTrue(hasContent(storage, "file.txt", "remote edit"));
        disconnect();
        upload("barrier.txt", "barrier");
        connect();
        await(() -> hasContent(local, "barrier.txt", "barrier"));
        assertEquals(first.id(), conflict("file.txt").id());
        assertEquals(ConflictRegistry.Status.OPEN, conflict("file.txt").status());
    }

    @Test
    void missingTrackedLocalFileAndObservedDeletionArePreserved() throws Exception {
        establish("tracked.txt", "base");
        disconnect();
        Files.delete(local.resolve("tracked.txt"));
        Files.writeString(local.resolve("observed.txt"), "local-only then deleted");
        Files.delete(local.resolve("observed.txt"));
        event(DELETE, "observed.txt");
        upload("observed.txt", "remote");
        upload("brand-new.txt", "remote");
        connect();
        await(() -> conflict("tracked.txt") != null && conflict("observed.txt") != null
            && hasContent(local, "brand-new.txt", "remote"));
        assertFalse(Files.exists(local.resolve("tracked.txt")));
        assertFalse(Files.exists(local.resolve("observed.txt")));
        assertEquals(ConflictRegistry.Status.REQUIRES_RECONCILIATION, conflict("tracked.txt").status());
    }

    @Test
    void missingServerFileDoesNotBecomeCreateButObservedNewLocalFileDoes() throws Exception {
        establish("tracked.txt", "base");
        disconnect();
        Files.delete(storage.resolve("tracked.txt"));
        Files.writeString(local.resolve("unknown.txt"), "untracked");
        Files.writeString(local.resolve("created.txt"), "offline creation");
        event(CREATE, "created.txt");
        connect();
        await(() -> conflict("tracked.txt") != null && conflict("unknown.txt") != null
            && hasContent(storage, "created.txt", "offline creation"));
        assertFalse(Files.exists(storage.resolve("tracked.txt")));
        assertFalse(Files.exists(storage.resolve("unknown.txt")));
    }

    @Test
    void restartedManagerOnlyReestablishesBaselineForVerifiedEqualFiles() throws Exception {
        establish("equal.txt", "equal");
        upload("divergent.txt", "base");
        await(() -> hasContent(local, "divergent.txt", "base"));
        disconnect();
        manager.close();
        createManager();
        Files.writeString(local.resolve("divergent.txt"), "unverified local");
        modify("divergent.txt", "unverified remote");
        connect();
        await(() -> confirmations.get() > 0 && conflict("divergent.txt") != null);
        assertEquals(ConflictRegistry.Status.REQUIRES_RECONCILIATION, conflict("divergent.txt").status());
        Files.writeString(local.resolve("equal.txt"), "new edit after verified equality");
        event(MODIFY, "equal.txt");
        await(() -> hasContent(storage, "equal.txt", "new edit after verified equality"));
        assertTrue(hasContent(local, "divergent.txt", "unverified local"));
        assertTrue(hasContent(storage, "divergent.txt", "unverified remote"));
    }

    @Test
    void disconnectDuringDownloadDiscardsStagingAndDoesNotRecordConflict() throws Exception {
        writeLargeServerFile("large.bin");
        connect();
        await(this::hasDownloadStaging);
        disconnect();
        await(() -> !hasDownloadStaging());
        assertFalse(Files.exists(local.resolve("large.bin")));
        assertTrue(manager.conflicts().snapshot().isEmpty());
        connect();
        await(() -> confirmations.get() == 1);
        assertEquals(Files.size(storage.resolve("large.bin")), Files.size(local.resolve("large.bin")));
    }

    @Test
    void replacementRechecksLocalContentAfterDownloading() throws Exception {
        establish("file.txt", "base");
        disconnect();
        writeLargeServerFile("file.txt");
        connect();
        await(this::hasDownloadStaging);
        Files.writeString(local.resolve("file.txt"), "edit during the download");
        await(() -> conflict("file.txt") != null && !hasDownloadStaging());
        assertEquals(ConflictRegistry.Status.OPEN, conflict("file.txt").status());
        assertTrue(hasContent(local, "file.txt", "edit during the download"));
        assertEquals(24L * 1024 * 1024, Files.size(storage.resolve("file.txt")));
    }

    @Test
    void repeatedReconnectsLeaveOnlyCurrentSessionToConfirm() throws Exception {
        establish("file.txt", "base");
        disconnect();
        modify("file.txt", "server edit");
        for (int index = 0; index < 4; index++) {
            connect();
            disconnect();
        }
        connect();
        await(() -> hasContent(local, "file.txt", "server edit"));
        assertTrue(manager.conflicts().snapshot().isEmpty());
    }

    @Test
    void reconciliationAndRealWatcherKeepConditionalUpdatesWorking() throws Exception {
        try (var coordinator = SyncCoordinator.forCreateSync(local, manager)) {
            coordinator.start();
            establish("file.txt", "base");
            disconnect();
            modify("file.txt", "server edit while offline");
            connect();
            Files.writeString(local.resolve("parallel.txt"), "watcher creation");
            await(() -> hasContent(storage, "parallel.txt", "watcher creation")
                && hasContent(local, "file.txt", "server edit while offline"));
            Files.writeString(local.resolve("file.txt"), "watcher edit");
            await(() -> hasContent(storage, "file.txt", "watcher edit"));
            assertTrue(manager.conflicts().snapshot().stream().noneMatch(entry -> entry.path().equals("file.txt")));
        }
    }

    @Test
    void manualAndLiveCreateModifyTransfersStillWork() throws Exception {
        establish("tracked.txt", "base");
        upload("manual.txt", "manual upload");
        await(() -> hasContent(local, "manual.txt", "manual upload"));
        modify("tracked.txt", "live remote modify");
        await(() -> hasContent(local, "tracked.txt", "live remote modify"));
        Files.writeString(local.resolve("local-create.txt"), "live local CREATE");
        event(CREATE, "local-create.txt");
        await(() -> hasContent(storage, "local-create.txt", "live local CREATE"));
        Files.writeString(local.resolve("local-create.txt"), "live local MODIFY");
        event(MODIFY, "local-create.txt");
        await(() -> hasContent(storage, "local-create.txt", "live local MODIFY"));
        var transfer = new FileTransferClient(peer);
        peer.lockFileOperation();
        try {
            var file = transfer.list().stream().filter(entry -> entry.name().equals("manual.txt")).findFirst().orElseThrow();
            transfer.download(file, manual, () -> false, (bytes, total) -> {});
            assertTrue(hasContent(manual, "manual.txt", "manual upload"));
            assertThrows(FileTransferClient.Failure.class,
                () -> transfer.upload(manual.resolve("manual.txt"), "tracked.txt", java.util.Optional.empty(),
                    () -> false, (bytes, total) -> {}));
            assertTrue(hasContent(storage, "tracked.txt", "live remote modify"));
        } finally {
            peer.unlockFileOperation();
        }
    }

    @Test
    void filesAndConflictsRefreshInJavaFxWindow() throws Exception {
        startJavaFx();
        upload("divergent.txt", "server\nshared\nserver second");
        Files.writeString(local.resolve("divergent.txt"), "local\nshared\nlocal second");
        Files.writeString(local.resolve("offline.txt"), "offline creation");
        event(CREATE, "offline.txt");
        record Window(Stage stage, Parent root, ShellController controller) {}
        var window = fx(() -> {
            var loader = new FXMLLoader(Main.class.getResource("view/shell.fxml"));
            Parent root = loader.load();
            ShellController controller = loader.getController();
            controller.bindAutomaticSync(manager);
            var stage = new Stage();
            stage.setScene(new Scene(root, 1050, 650));
            stage.show();
            ((Button) root.lookup("#devices")).fire();
            ((TextField) root.lookup("#ip")).setText("127.0.0.1");
            ((TextField) root.lookup("#port")).setText(Integer.toString(port));
            ((Button) root.lookup("#connect")).fire();
            ((Button) root.lookup("#files")).fire();
            return new Window(stage, root, controller);
        });
        try {
            await(() -> fx(() -> {
                var table = (TableView<?>) window.root().lookup("#content .table-view");
                return table != null && table.getItems().stream().anyMatch(item ->
                    item instanceof FileMetadata file && file.name().equals("offline.txt"));
            }));
            fx(() -> {
                ((Button) window.root().lookup("#conflicts")).fire();
                return null;
            });
            await(() -> fx(() -> {
                var box = (ComboBox<?>) window.root().lookup("#conflict");
                return box != null && box.getItems().stream().anyMatch(item ->
                    item instanceof ConflictRegistry.Entry entry && entry.path().equals("divergent.txt"));
            }));
            await(() -> fx(() -> !window.root().lookup("#compare").isDisabled()));
            fx(() -> {
                ((Button) window.root().lookup("#compare")).fire();
                return null;
            });
            await(() -> fx(() -> openedComparison() != null
                && ((TableView<?>) openedComparison().lookup("#diffTable")).getItems().size() == 3));
            fx(() -> {
                var dialog = openedComparison();
                var diff = (TableView<?>) dialog.lookup("#diffTable");
                assertEquals(0, diff.getSelectionModel().getSelectedIndex());
                ((Button) dialog.lookup("#diffNext")).fire();
                assertEquals(2, diff.getSelectionModel().getSelectedIndex());
                ((Button) dialog.lookup("#diffPrevious")).fire();
                assertEquals(0, diff.getSelectionModel().getSelectedIndex());
                ((Button) dialog.lookupButton(dialog.getButtonTypes().getFirst())).fire();
                return null;
            });
            assertTrue(hasContent(local, "divergent.txt", "local\nshared\nlocal second"));
            verifyHistoryCallbackOnJavaFxThread();
        } finally {
            fx(() -> {
                window.controller().closeConnection();
                window.stage().close();
                return null;
            });
        }
    }

    private static javafx.scene.control.DialogPane openedComparison() {
        return javafx.stage.Window.getWindows().stream().filter(javafx.stage.Window::isShowing)
            .map(window -> window.getScene().getRoot())
            .filter(javafx.scene.control.DialogPane.class::isInstance)
            .map(javafx.scene.control.DialogPane.class::cast)
            .filter(pane -> pane.lookup("#diffTable") != null)
            .findFirst().orElse(null);
    }

    @Test
    void closingComparisonDialogDuringRealDownloadDiscardsTheResponse() throws Exception {
        startJavaFx();
        upload("compare.txt", "server\nversion");
        Files.writeString(local.resolve("compare.txt"), "unsent local");
        var downloading = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (var relay = new ServerSocket(0); var window = historyWindow()) {
            relay.setSoTimeout(10_000);
            var proxy = new FutureTask<Void>(() -> {
                try (var downstream = relay.accept(); var upstream = new java.net.Socket("127.0.0.1", port)) {
                    Thread.ofVirtual().start(() -> {
                        try {
                            relayFrames(downstream, upstream, message -> {});
                        } catch (IOException ignored) {
                            // The application closes the session at the end of the test.
                        }
                    });
                    relayFrames(upstream, downstream, message -> {
                        if (message.type() == MessageType.FILE_CHUNK) {
                            downloading.countDown();
                            try {
                                if (!release.await(8, TimeUnit.SECONDS)) {
                                    throw new IOException("Test did not release the buffered download chunk");
                                }
                            } catch (InterruptedException exception) {
                                Thread.currentThread().interrupt();
                                throw new IOException(exception);
                            }
                        }
                    });
                } catch (java.net.SocketException ignored) {
                    // Disconnect closes the relay after the assertions.
                }
                return null;
            });
            Thread.ofVirtual().start(proxy);
            try {
                fx(() -> {
                    window.controller().bindAutomaticSync(manager);
                    window.connect(relay.getLocalPort());
                    window.click("#conflicts");
                    return null;
                });
                await(() -> fx(() -> !window.root().lookup("#compare").isDisabled()));
                fx(() -> {
                    window.click("#compare");
                    return null;
                });
                assertTrue(downloading.await(8, TimeUnit.SECONDS));
                fx(() -> {
                    var dialog = openedComparison();
                    assertNotNull(dialog);
                    assertTrue(((TableView<?>) dialog.lookup("#diffTable")).getItems().isEmpty());
                    ((Button) dialog.lookupButton(dialog.getButtonTypes().getFirst())).fire();
                    return null;
                });
                release.countDown();
                await(() -> fx(() -> openedComparison() == null && !window.root().lookup("#compare").isDisabled()));
                assertTrue(hasContent(local, "compare.txt", "unsent local"));
                fx(() -> {
                    window.click("#devices");
                    window.click("#disconnect");
                    return null;
                });
                proxy.get(10, TimeUnit.SECONDS);
            } finally {
                release.countDown();
            }
        }
    }

    @FunctionalInterface
    private interface FrameCheck {
        void accept(ProtocolMessage message) throws IOException;
    }

    private static void relayFrames(java.net.Socket input, java.net.Socket output, FrameCheck check) throws IOException {
        var source = new java.io.DataInputStream(input.getInputStream());
        var destination = new java.io.DataOutputStream(output.getOutputStream());
        for (ProtocolMessage message; (message = MessageCodec.read(source)) != null;) {
            check.accept(message);
            MessageCodec.write(destination, message);
            if (message.type() == MessageType.DISCONNECT) {
                output.shutdownOutput();
                return;
            }
        }
        output.shutdownOutput();
    }

    @Test
    void historyNavigatesFromFilesAndRefreshesAfterModifyAndReconnect() throws Exception {
        startJavaFx();
        String historyPath = ("documents-with-a-very-long-parent-name/").repeat(7) + "history.txt";
        upload(historyPath, "original");
        modify(historyPath, "second");
        modify(historyPath, "third");
        upload("empty.txt", "no historical versions");
        try (var window = historyWindow()) {
            fx(() -> {
                window.click("#history");
                assertTrue(window.status().contains("Kết nối"));
                window.connect(port);
                window.click("#history");
                return null;
            });
            await(() -> fx(() -> window.status().startsWith("Chọn một tệp")));
            await(() -> fx(() -> window.choices().getItems().contains(historyPath)));
            fx(() -> {
                window.click("#files");
                var table = (TableView<?>) window.root().lookup("#content .table-view");
                int index = java.util.stream.IntStream.range(0, table.getItems().size())
                    .filter(i -> ((FileMetadata) table.getItems().get(i)).name().equals(historyPath))
                    .findFirst().orElseThrow();
                table.getSelectionModel().select(index);
                window.click("#openVersionHistory");
                assertEquals(historyPath, window.choices().getValue());
                return null;
            });
            await(() -> fx(() -> window.versions().getItems().size() == 2));
            fx(() -> {
                var rows = window.versions();
                assertEquals(6, rows.getItems().getFirst().size());
                assertEquals(8, rows.getItems().getLast().size());
                rows.getSelectionModel().selectFirst();
                assertEquals(rows.getItems().getFirst().sha256(), ((TextField) window.root().lookup("#checksum")).getText());
                assertEquals(historyPath, ((TextField) window.root().lookup("#selectedPath")).getText());
                window.root().applyCss();
                window.root().layout();
                var restoreButton = (Button) window.root().lookup("#restore");
                assertFalse(restoreButton.isDisabled());
                assertTrue(restoreButton.getWidth() >= 105, "The selected path must not squeeze the Restore action");
                return null;
            });
            modify(historyPath, "fourth");
            await(() -> fx(() -> window.versions().getItems().size() == 3));
            fx(() -> {
                window.choices().setValue("empty.txt");
                return null;
            });
            await(() -> fx(() -> window.status().contains("chưa có phiên bản") && window.versions().getItems().isEmpty()));
            fx(() -> {
                window.click("#devices");
                window.click("#disconnect");
                window.click("#history");
                assertTrue(window.status().contains("Kết nối"));
                window.connect(port);
                window.click("#history");
                window.choices().setValue(historyPath);
                return null;
            });
            await(() -> fx(() -> window.versions().getItems().size() == 3));
            fx(() -> {
                window.click("#historyRefresh");
                return null;
            });
            await(() -> fx(() -> window.versions().getItems().size() == 3));
        }
    }

    @Test
    void protocolRestoreSyncsCleanClientAndProtectsUnsentEdit() throws Exception {
        establish("file.txt", "original");
        modify("file.txt", "second");
        await(() -> hasContent(local, "file.txt", "second"));
        var older = history("file.txt").getFirst();
        var stale = assertThrows(FileTransferClient.Failure.class, () -> restore("file.txt", older, "original"));
        assertEquals(FileTransferCodec.RESULT_CONFLICT, stale.resultCode());
        restore("file.txt", older, "second");
        await(() -> hasContent(local, "file.txt", "original"));
        assertTrue(hasContent(storage, "file.txt", "original"));
        var restoredHistory = history("file.txt");
        assertEquals(2, restoredHistory.size());
        assertEquals(digest("second"), restoredHistory.getFirst().sha256());
        assertEquals(older, restoredHistory.getLast());
        restore("file.txt", older, "original");
        assertEquals(3, history("file.txt").size());
        Files.writeString(local.resolve("file.txt"), "unsent local edit");
        restore("file.txt", restoredHistory.getFirst(), "original");
        await(() -> conflict("file.txt") != null);
        assertTrue(hasContent(storage, "file.txt", "second"));
        assertTrue(hasContent(local, "file.txt", "unsent local edit"));
        disconnect();
        peer.close();
        server.close();
        startServer();
        assertEquals(4, history("file.txt").size());
        assertTrue(hasContent(storage, "file.txt", "second"));
    }

    @Test
    void javaFxRestoreRequiresConfirmationAndReportsSuccessAndStaleBase() throws Exception {
        startJavaFx();
        upload("file.txt", "original");
        modify("file.txt", "second");
        try (var window = historyWindow()) {
            fx(() -> {
                window.connect(port);
                window.click("#history");
                return null;
            });
            await(() -> fx(() -> window.choices().getItems().contains("file.txt")));
            fx(() -> {
                window.choices().setValue("file.txt");
                return null;
            });
            openRestoreDialog(window);
            chooseRestore(false);
            await(() -> fx(() -> window.versions().getItems().size() == 1));
            assertTrue(hasContent(storage, "file.txt", "second"));

            openRestoreDialog(window);
            modify("file.txt", "changed during confirmation");
            chooseRestore(true);
            await(() -> fx(() -> restoreFeedback(window).contains("đã thay đổi")));
            assertTrue(hasContent(storage, "file.txt", "changed during confirmation"));
            await(() -> fx(() -> window.versions().getItems().size() == 2));

            openRestoreDialog(window);
            chooseRestore(true);
            await(() -> fx(() -> restoreFeedback(window).startsWith("Đã khôi phục")
                && window.versions().getItems().size() == 3));
            assertTrue(hasContent(storage, "file.txt", "original"));
        }
    }

    private static void openRestoreDialog(HistoryWindow window) throws Exception {
        await(() -> fx(() -> !window.versions().getItems().isEmpty()));
        fx(() -> {
            window.versions().getSelectionModel().selectLast();
            return null;
        });
        await(() -> fx(() -> !window.root().lookup("#restore").isDisabled()));
        fx(() -> {
            window.click("#restore");
            return null;
        });
        await(() -> fx(() -> restoreDialog() != null));
    }

    private static String restoreFeedback(HistoryWindow window) {
        return ((javafx.scene.control.Label) window.root().lookup("#restoreStatus")).getText();
    }

    private static javafx.scene.control.DialogPane restoreDialog() {
        return javafx.stage.Window.getWindows().stream().filter(javafx.stage.Window::isShowing)
            .map(window -> window.getScene().getRoot())
            .filter(javafx.scene.control.DialogPane.class::isInstance)
            .map(javafx.scene.control.DialogPane.class::cast).findFirst().orElse(null);
    }

    private static void chooseRestore(boolean confirm) throws Exception {
        fx(() -> {
            var dialog = restoreDialog();
            assertNotNull(dialog);
            var type = dialog.getButtonTypes().stream()
                .filter(button -> button.getButtonData().isCancelButton() != confirm).findFirst().orElseThrow();
            ((Button) dialog.lookupButton(type)).fire();
            return null;
        });
    }

    private java.util.List<FileVersion> history(String path) throws Exception {
        peer.lockFileOperation();
        try {
            return new FileTransferClient(peer).listVersions(path);
        } finally {
            peer.unlockFileOperation();
        }
    }

    private void restore(String path, FileVersion version, String expected) throws Exception {
        peer.lockFileOperation();
        try {
            new FileTransferClient(peer).restoreVersion(path, version.id(), digest(expected));
        } finally {
            peer.unlockFileOperation();
        }
    }

    private static String digest(String contents) {
        return java.util.HexFormat.of().formatHex(FileTransferCodec.sha256()
            .digest(contents.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
    }

    @Test
    void historyDiscardsInFlightSelectionAndShowsUnsupportedAndFailedStates() throws Exception {
        startJavaFx();
        var firstRequest = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        try (var socket = new ServerSocket(0);
             var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();
             var window = historyWindow()) {
            socket.setSoTimeout(12_000);
            var exchange = executor.submit(() -> {
                try (var accepted = socket.accept()) {
                    accepted.setSoTimeout(12_000);
                    serveHistoryFixture(new java.io.DataInputStream(accepted.getInputStream()),
                        new java.io.DataOutputStream(accepted.getOutputStream()), firstRequest, release, true);
                }
                try (var accepted = socket.accept()) {
                    accepted.setSoTimeout(12_000);
                    serveHistoryFixture(new java.io.DataInputStream(accepted.getInputStream()),
                        new java.io.DataOutputStream(accepted.getOutputStream()), firstRequest, release, false);
                }
                return null;
            });
            try {
                fx(() -> {
                    window.connect(socket.getLocalPort());
                    window.click("#history");
                    return null;
                });
                await(() -> fx(() -> window.choices().getItems().size() == 2));
                var painted = new java.util.ArrayList<String>();
                fx(() -> {
                    window.versions().getItems().addListener((javafx.collections.ListChangeListener<FileVersion>) change -> {
                        assertTrue(Platform.isFxApplicationThread());
                        while (change.next()) {
                            change.getAddedSubList().forEach(version -> painted.add(version.relativePath()));
                        }
                    });
                    window.choices().setValue("a.txt");
                    return null;
                });
                assertTrue(firstRequest.await(8, TimeUnit.SECONDS));
                fx(() -> {
                    assertTrue(window.status().startsWith("Đang tải"));
                    window.choices().setValue("b.txt");
                    return null;
                });
                release.countDown();
                await(() -> fx(() -> window.status().startsWith("Không tải được")));
                fx(() -> {
                    assertTrue(painted.isEmpty());
                    window.click("#devices");
                    window.click("#disconnect");
                    window.connect(socket.getLocalPort());
                    window.click("#history");
                    return null;
                });
                await(() -> fx(() -> window.status().contains("không hỗ trợ")));
                fx(() -> {
                    assertTrue(window.root().lookup("#historyRefresh").isDisabled());
                    window.click("#devices");
                    window.click("#disconnect");
                    return null;
                });
                exchange.get(8, TimeUnit.SECONDS);
            } finally {
                release.countDown();
            }
        }
    }

    private static void serveHistoryFixture(java.io.DataInputStream input, java.io.DataOutputStream output,
                                            CountDownLatch firstRequest, CountDownLatch release, boolean supported)
            throws Exception {
        assertEquals(MessageType.HELLO, MessageCodec.read(input).type());
        MessageCodec.write(output, new ProtocolMessage(MessageType.WELCOME, java.util.UUID.randomUUID()
            + (supported ? FileTransferCodec.VERSION_HISTORY_CAPABILITY : "")));
        for (ProtocolMessage message; (message = MessageCodec.read(input)) != null;) {
            if (message.type() == MessageType.DISCONNECT) {
                return;
            }
            if (message.type() == MessageType.PING) {
                MessageCodec.write(output, new ProtocolMessage(MessageType.PONG));
                continue;
            }
            var request = FileTransferCodec.decode(message);
            byte[] payload;
            MessageType response;
            if (message.type() == MessageType.FILE_LIST_REQUEST) {
                response = MessageType.FILE_LIST_RESPONSE;
                payload = FileTransferCodec.body(data -> {
                    data.writeByte(0);
                    data.writeInt(2);
                    FileTransferCodec.writeMetadata(data, new FileMetadata("a.txt", 1, 0));
                    FileTransferCodec.writeMetadata(data, new FileMetadata("b.txt", 1, 0));
                });
            } else {
                assertTrue(supported);
                assertEquals(MessageType.VERSION_LIST_REQUEST, message.type());
                response = MessageType.VERSION_LIST_RESPONSE;
                String path = FileTransferCodec.readText(request.body());
                if (path.equals("a.txt")) {
                    firstRequest.countDown();
                    assertTrue(release.await(8, TimeUnit.SECONDS));
                    payload = FileTransferCodec.versionListResult(java.util.List.of(new FileVersion(
                        java.util.UUID.randomUUID(), path, "0".repeat(64), 1, 0, Instant.EPOCH)));
                } else {
                    assertEquals("b.txt", path);
                    payload = FileTransferCodec.result("fixture error");
                }
            }
            MessageCodec.write(output, FileTransferCodec.encode(response, request.requestId(), request.transferId(), payload));
        }
    }

    private static boolean javaFxStarted;

    private static synchronized void startJavaFx() throws Exception {
        if (!javaFxStarted) {
            var started = new CountDownLatch(1);
            Platform.startup(() -> {
                Platform.setImplicitExit(false);
                started.countDown();
            });
            assertTrue(started.await(10, TimeUnit.SECONDS));
            javaFxStarted = true;
        }
    }

    private static HistoryWindow historyWindow() throws Exception {
        return fx(() -> {
            var loader = new FXMLLoader(Main.class.getResource("view/shell.fxml"));
            Parent root = loader.load();
            var stage = new Stage();
            stage.setScene(new Scene(root, 1050, 680));
            stage.show();
            return new HistoryWindow(stage, root, loader.getController());
        });
    }

    private record HistoryWindow(Stage stage, Parent root, ShellController controller) implements AutoCloseable {
        void click(String selector) {
            ((Button) root.lookup(selector)).fire();
        }

        void connect(int port) {
            click("#devices");
            ((TextField) root.lookup("#ip")).setText("127.0.0.1");
            ((TextField) root.lookup("#port")).setText(Integer.toString(port));
            click("#connect");
        }

        String status() {
            return ((javafx.scene.control.Label) root.lookup("#historyStatus")).getText();
        }

        @SuppressWarnings("unchecked")
        ComboBox<String> choices() {
            return (ComboBox<String>) root.lookup("#file");
        }

        @SuppressWarnings("unchecked")
        TableView<FileVersion> versions() {
            return (TableView<FileVersion>) root.lookup("#versions");
        }

        @Override public void close() throws Exception {
            fx(() -> {
                controller.closeConnection();
                stage.close();
                return null;
            });
        }
    }

    private void verifyHistoryCallbackOnJavaFxThread() throws Exception {
        upload("history.txt", "original");
        modify("history.txt", "current");
        var connection = fx(() -> {
            var service = new ConnectionService();
            service.connect("127.0.0.1", port);
            return service;
        });
        try {
            await(() -> fx(() -> connection.connectedClient() != null));
            var received = new java.util.concurrent.CompletableFuture<java.util.List<FileVersion>>();
            fx(() -> {
                new FileService(connection).listVersions("history.txt", versions -> {
                    assertTrue(Platform.isFxApplicationThread());
                    received.complete(versions);
                }, received::completeExceptionally);
                return null;
            });
            var history = received.get(10, TimeUnit.SECONDS);
            assertEquals(1, history.size());
            assertEquals("history.txt", history.getFirst().relativePath());
            assertEquals("original".length(), history.getFirst().size());
        } finally {
            fx(() -> {
                connection.close();
                return null;
            });
        }
    }

    private void createManager() {
        confirmations.set(0);
        manager = new SyncTransferManager(local);
        manager.setUploadCommittedListener(connection -> confirmations.incrementAndGet());
    }

    private void connect() throws IOException {
        var connection = new TcpClient();
        client = connection;
        connection.connect("127.0.0.1", port, "test-client", notification -> {
            if (!dropNextNotification.getAndSet(false)) {
                manager.onRemoteChange(connection, notification);
            }
        });
        assertTrue(connection.supportsReconciliation());
        manager.setSession(connection);
    }

    private void disconnect() throws IOException {
        manager.setSession(null);
        if (client != null) {
            client.close();
            client = null;
        }
    }

    private void establish(String path, String content) throws Exception {
        upload(path, content);
        connect();
        await(() -> confirmations.get() > 0 && hasContent(local, path, content));
    }

    private void upload(String path, String content) throws Exception {
        send(path, content, null);
    }

    private void modify(String path, String content) throws Exception {
        String base = SyncFileRegistry.read(storage.resolve(path), () -> false).sha256();
        send(path, content, base);
    }

    private void send(String path, String content, String base) throws Exception {
        Path source = Files.writeString(manual.resolve("source.txt"), content);
        var transfer = new FileTransferClient(peer);
        peer.lockFileOperation();
        try {
            if (base == null) {
                transfer.upload(source, path, java.util.Optional.empty(), () -> false, (bytes, total) -> {});
            } else {
                transfer.uploadModified(source, path, base, () -> false, (bytes, total) -> {});
            }
        } finally {
            peer.unlockFileOperation();
        }
    }

    private void event(FileChangeEvent.Type type, String path) {
        manager.onLocalChange(new FileChangeEvent(type, Path.of(path), Instant.now(), OptionalLong.empty(), true));
    }

    private ConflictRegistry.Entry conflict(String path) {
        return manager.conflicts().snapshot().stream().filter(entry -> entry.path().equals(path)).findFirst().orElse(null);
    }

    private boolean hasDownloadStaging() throws IOException {
        try (var files = Files.list(local)) {
            return files.anyMatch(file -> file.getFileName().toString().startsWith(".download-"));
        }
    }

    private void writeLargeServerFile(String path) throws IOException {
        try (var large = new RandomAccessFile(storage.resolve(path).toFile(), "rw")) {
            large.setLength(24L * 1024 * 1024);
        }
    }

    private static boolean hasContent(Path directory, String path, String content) throws IOException {
        Path file = directory.resolve(path);
        return Files.isRegularFile(file) && Files.readString(file).equals(content);
    }

    private static void await(Callable<Boolean> check) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (!check.call()) {
            assertTrue(System.nanoTime() < deadline, "Timed out waiting for synchronization");
            Thread.sleep(10);
        }
    }

    private static <T> T fx(Callable<T> action) throws Exception {
        var task = new FutureTask<>(action);
        Platform.runLater(task);
        return task.get(10, TimeUnit.SECONDS);
    }
}
