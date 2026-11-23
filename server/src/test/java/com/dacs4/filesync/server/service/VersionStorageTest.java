package com.dacs4.filesync.server.service;

import com.dacs4.filesync.shared.protocol.FileTransferCodec.SafeRoot;
import com.dacs4.filesync.shared.exception.ProtocolException;
import com.dacs4.filesync.shared.protocol.MessageCodec;
import com.dacs4.filesync.shared.dto.FileVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.dacs4.filesync.shared.protocol.FileTransferCodec.*;
import static com.dacs4.filesync.shared.protocol.MessageType.UPLOAD_INIT;
import static com.dacs4.filesync.shared.protocol.MessageType.UPLOAD_RESULT;
import static com.dacs4.filesync.shared.protocol.MessageType.VERSION_LIST_REQUEST;
import static com.dacs4.filesync.shared.protocol.MessageType.VERSION_LIST_RESPONSE;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.junit.jupiter.api.Assertions.*;

class VersionStorageTest {
    @TempDir Path temporary;
    private static final String DOCUMENT = "nested/doc.txt";
    private static final String ORIGINAL = "original ✓";

    @Test
    void firstModifyPreservesOriginalContentAndMetadata() throws Exception {
        try (var storage = open()) {
            Path original = create(storage);
            long modified = Files.getLastModifiedTime(original).toMillis();
            assertEquals(original, storage.replace(stage(storage, "second"), DOCUMENT, hash(ORIGINAL)));
            assertEquals("second", Files.readString(original));
            var history = committed();
            assertEquals(1, history.size());
            var metadata = assertVersion(history.getFirst(), ORIGINAL);
            assertEquals(Long.toString(modified), metadata.getProperty("modifiedMillis"));
            assertNotNull(Instant.parse(metadata.getProperty("archivedAt")));
            assertNoJournal();
        }
    }

    @Test
    void restoresOlderAndLatestVersionsAndRetainsHistoryAcrossRestart() throws Exception {
        UUID older;
        try (var storage = open()) {
            Path active = create(storage);
            assertNotNull(storage.replace(stage(storage, "second"), DOCUMENT, hash(ORIGINAL)));
            older = storage.listVersions(DOCUMENT).getFirst().id();
            assertNotNull(storage.replace(stage(storage, "third"), DOCUMENT, hash("second")));
            UUID latest = storage.listVersions(DOCUMENT).getFirst().id();
            assertEquals(latest, storage.restore(DOCUMENT, latest, hash("third")).id());
            assertEquals("second", Files.readString(active));
            assertEquals(older, storage.restore(DOCUMENT, older, hash("second")).id());
            assertArrayEquals(hash(ORIGINAL), storage.version(DOCUMENT).hash());
            assertEquals(older, storage.restore(DOCUMENT, older, hash(ORIGINAL)).id());
            assertEquals(5, storage.listVersions(DOCUMENT).size());
            assertVersion(archive().resolve(older.toString()), ORIGINAL);
            assertNoJournal();
        }
        try (var storage = open()) {
            assertEquals(5, storage.listVersions(DOCUMENT).size());
            assertEquals(older, storage.listVersions(DOCUMENT).stream()
                .filter(version -> version.id().equals(older)).findFirst().orElseThrow().id());
            assertArrayEquals(hash(ORIGINAL), storage.version(DOCUMENT).hash());
        }
    }

    @Test
    void restoreRejectsWrongBaseMissingCorruptAndOtherPathVersions() throws Exception {
        try (var storage = open()) {
            Path active = create(storage);
            storage.replace(stage(storage, "second"), DOCUMENT, hash(ORIGINAL));
            UUID selected = storage.listVersions(DOCUMENT).getFirst().id();
            assertNull(storage.restore(DOCUMENT, selected, hash("stale")));
            assertThrows(IOException.class, () -> storage.restore(DOCUMENT, UUID.randomUUID(), hash("second")));
            storage.commit(stage(storage, "other"), "other.txt");
            assertThrows(IOException.class, () -> storage.restore("other.txt", selected, hash("other")));
            Files.writeString(archive().resolve(selected.toString()).resolve("content"), "damage");
            assertThrows(IOException.class, () -> storage.restore(DOCUMENT, selected, hash("second")));
            assertEquals("second", Files.readString(active));
            assertEquals("other", Files.readString(storage.file("other.txt", false)));
            assertEquals(1, committed().size());
            assertNoJournal();
        }
    }

    @Test
    void interruptedRestoreAtAtomicMoveKeepsCurrentAndArchive() throws Exception {
        try (var storage = open()) {
            create(storage);
            storage.replace(stage(storage, "second"), DOCUMENT, hash(ORIGINAL));
        }
        try (var storage = new FileStorageService(temporary.resolve("active"), 10_000, (source, target) -> {
            throw new java.io.InterruptedIOException("Injected interruption at atomic publication");
        })) {
            UUID selected = storage.listVersions(DOCUMENT).getFirst().id();
            assertThrows(java.io.InterruptedIOException.class, () -> storage.restore(DOCUMENT, selected, hash("second")));
            assertArrayEquals(hash("second"), storage.version(DOCUMENT).hash());
            assertEquals(1, committed().size());
            assertVersion(committed().getFirst(), ORIGINAL);
            assertNoJournal();
        }
    }

    @Test
    void concurrentRestoreAndModifyWithSameBaselineHaveOneWinner() throws Exception {
        try (var storage = open(); var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            create(storage);
            storage.replace(stage(storage, "second"), DOCUMENT, hash(ORIGINAL));
            UUID selected = storage.listVersions(DOCUMENT).getFirst().id();
            Path upload = stage(storage, "third");
            var ready = new CountDownLatch(2);
            var start = new CountDownLatch(1);
            var restore = tasks.submit(() -> {
                ready.countDown();
                start.await();
                return storage.restore(DOCUMENT, selected, hash("second"));
            });
            var modify = tasks.submit(() -> {
                ready.countDown();
                start.await();
                return storage.replace(upload, DOCUMENT, hash("second"));
            });
            boolean bothReady = ready.await(5, TimeUnit.SECONDS);
            start.countDown();
            assertTrue(bothReady);
            assertNotEquals(restore.get(5, TimeUnit.SECONDS) == null, modify.get(5, TimeUnit.SECONDS) == null);
            assertEquals(2, storage.listVersions(DOCUMENT).size());
            assertNoJournal();
        }
    }

    @Test
    void successiveModifiesKeepDistinctVersionsAcrossRestart() throws Exception {
        List<Path> history;
        try (var storage = open()) {
            create(storage);
            assertNotNull(storage.replace(stage(storage, "second"), DOCUMENT, hash(ORIGINAL)));
            assertNotNull(storage.replace(stage(storage, "third"), DOCUMENT, hash("second")));
            history = committed();
            assertEquals(2, history.size());
        }
        try (var storage = open()) {
            assertEquals(history, committed());
            assertEquals("third", Files.readString(storage.file(DOCUMENT, false)));
            for (Path entry : history) {
                String content = Files.readString(entry.resolve("content"));
                assertTrue(List.of(ORIGINAL, "second").contains(content));
                assertVersion(entry, content);
            }
            assertEquals(List.of(ORIGINAL, "second"),
                history.stream().map(entry -> read(entry.resolve("content"))).sorted().toList());
        }
    }

    @Test
    void wrongBaselineReturnsProtocolConflictAndCommitsNothing() throws Exception {
        try (var storage = open();
             var transfers = new FileTransferService(storage, true, true, UUID.randomUUID(), (path, hash) -> {},
                 event -> {})) {
            var active = create(storage);
            UUID request = UUID.randomUUID();
            UUID transfer = UUID.randomUUID();
            var packet = decode(transfers.handle(encode(UPLOAD_INIT, request, transfer,
                modifyUploadInit(DOCUMENT, 5, HexFormat.of().formatHex(hash("wrong"))))));
            assertEquals(UPLOAD_RESULT, packet.type());
            assertEquals(RESULT_CONFLICT, packet.body().readUnsignedByte());
            assertNull(storage.replace(stage(storage, "wrong"), DOCUMENT, hash("wrong")));
            assertEquals(ORIGINAL, Files.readString(active));
            assertTrue(committed().isEmpty());
            assertNoJournal();
        }
    }

    @Test
    void concurrentModifiesWithTheSameBaseHaveOneWinner() throws Exception {
        try (var storage = open(); var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            var active = create(storage);
            Path first = stage(storage, "first");
            Path second = stage(storage, "second");
            var ready = new CountDownLatch(2);
            var start = new CountDownLatch(1);
            var a = tasks.submit(() -> race(storage, first, ready, start));
            var b = tasks.submit(() -> race(storage, second, ready, start));
            boolean bothReady = ready.await(5, TimeUnit.SECONDS);
            start.countDown();
            assertTrue(bothReady);
            assertNotEquals(a.get(5, TimeUnit.SECONDS) == null, b.get(5, TimeUnit.SECONDS) == null);
            assertTrue(List.of("first", "second").contains(Files.readString(active)));
            assertEquals(1, committed().size());
            assertVersion(committed().getFirst(), ORIGINAL);
            assertNoJournal();
        }
    }

    @Test
    void versionsStayOutsideListingsAndCreateStillRejectsDuplicates() throws Exception {
        try (var storage = open()) {
            create(storage);
            assertThrows(FileAlreadyExistsException.class, () -> storage.commit(stage(storage, "duplicate"), DOCUMENT));
            assertNotNull(storage.replace(stage(storage, "second"), DOCUMENT, hash(ORIGINAL)));
            assertEquals(List.of(DOCUMENT), storage.list(true).stream().map(file -> file.relativePath()).toList());
            assertTrue(storage.list(false).isEmpty());
            assertTrue(Files.isDirectory(archive()));
            assertFalse(archive().startsWith(temporary.resolve("active")));
            Path archivedVersion = committed().getFirst();
            assertThrows(IOException.class, () -> new FileStorageService(archivedVersion, 10_000));
        }
    }

    @Test
    void storageFailureWhileSealingVersionLeavesNoHistoryOrPendingEntry() throws Exception {
        Path previous;
        try (var storage = open()) {
            previous = create(storage);
        }
        Path invalidUpload = Files.createDirectory(temporary.resolve("not-an-upload"));
        try (var versions = new VersionStore(new SafeRoot(temporary.resolve("active")))) {
            assertThrows(IOException.class, () -> versions.prepare(DOCUMENT, previous,
                SafeRoot.regularAttributes(previous), hash(ORIGINAL), invalidUpload));
            assertEquals(ORIGINAL, Files.readString(previous));
            assertTrue(committed().isEmpty());
            assertNoJournal();
        }
    }

    @Test
    void atomicReplacementFailureLeavesActiveFileAndNoCommittedHistory() throws Exception {
        try (var storage = new FileStorageService(temporary.resolve("active"), 10_000, (source, target) -> {
            throw new AtomicMoveNotSupportedException(source.toString(), target.toString(), "failure injection");
        })) {
            Path active = create(storage);
            Path upload = stage(storage, "second");
            assertThrows(AtomicMoveNotSupportedException.class, () -> storage.replace(upload, DOCUMENT, hash(ORIGINAL)));
            assertEquals(ORIGINAL, Files.readString(active));
            assertEquals("second", Files.readString(upload));
            assertTrue(committed().isEmpty());
            assertNoJournal();
        }
    }

    @Test
    void restartRollsBackJournalIfActiveRenameDidNotTakePlace() throws Exception {
        Path original = journal(false);
        try (var storage = open()) {
            assertEquals(ORIGINAL, Files.readString(original));
            assertTrue(committed().isEmpty());
            assertNoJournal();
        }
    }

    @Test
    void restartPublishesJournalIfActiveRenameTookPlace() throws Exception {
        Path original = journal(true);
        try (var storage = open()) {
            assertEquals("second", Files.readString(original));
            assertEquals(1, committed().size());
            assertVersion(committed().getFirst(), ORIGINAL);
            assertNoJournal();
        }
    }

    @Test
    void corruptPendingContentFailsRecoveryWithoutChangingActiveData() throws Exception {
        Path active = journal(true);
        Path pending = pending();
        Files.writeString(pending.resolve("version/content"), "corrupt payload");
        assertThrows(IOException.class, this::open);
        assertEquals("second", Files.readString(active));
        assertTrue(committed().isEmpty());
        assertTrue(Files.isDirectory(pending));
    }

    @Test
    void invalidJournalPathCannotAccessFilesOutsideStorage() throws Exception {
        Path active = journal(true);
        Path outside = Files.writeString(temporary.resolve("outside.txt"), "outside");
        Path metadataFile = pending().resolve("version/metadata.properties");
        var metadata = new Properties();
        try (var input = Files.newInputStream(metadataFile)) {
            metadata.load(input);
        }
        metadata.setProperty("path", "../outside.txt");
        try (var output = Files.newOutputStream(metadataFile)) {
            metadata.store(output, null);
        }
        assertThrows(IOException.class, this::open);
        assertEquals("outside", Files.readString(outside));
        assertEquals("second", Files.readString(active));
    }

    private Path pending() throws IOException {
        try (var files = Files.newDirectoryStream(archive(), ".pending-*")) {
            return files.iterator().next();
        }
    }

    @Test
    void secondStorageOwnerCannotOpenArchiveUntilFirstCloses() throws Exception {
        try (var storage = open()) {
            assertThrows(IOException.class, this::open);
        }
        try (var reopened = open()) {
            assertTrue(reopened.list(true).isEmpty());
        }
    }

    @Test
    void historyIsOrderedAndScopedToTheExactActivePathAfterRestart() throws Exception {
        try (var storage = open()) {
            create(storage);
            assertTrue(storage.listVersions(DOCUMENT).isEmpty());
            storage.commit(stage(storage, "other"), "other.txt");
            storage.replace(stage(storage, "unrelated"), "other.txt", hash("other"));
            storage.replace(stage(storage, "second"), DOCUMENT, hash(ORIGINAL));
            var first = storage.listVersions(DOCUMENT).getFirst();
            assertEquals(DOCUMENT, first.relativePath());
            assertEquals(HexFormat.of().formatHex(hash(ORIGINAL)), first.sha256());
            assertEquals(ORIGINAL.getBytes(StandardCharsets.UTF_8).length, first.size());
            assertEquals(UUID.fromString(committed().stream().filter(entry -> entry.getFileName().toString()
                .equals(first.id().toString())).findFirst().orElseThrow().getFileName().toString()), first.id());
            storage.replace(stage(storage, "third"), DOCUMENT, hash("second"));
        }
        try (var storage = open()) {
            var history = storage.listVersions(DOCUMENT);
            assertEquals(List.of(HexFormat.of().formatHex(hash("second")), HexFormat.of().formatHex(hash(ORIGINAL))),
                history.stream().map(FileVersion::sha256).toList());
            assertTrue(storage.listVersions("missing.txt").isEmpty());
            assertThrows(ProtocolException.class, () -> storage.listVersions("../active/nested/doc.txt"));
        }
    }

    @Test
    void historySkipsCorruptAndIncompleteEntriesAndAnInFlightJournal() throws Exception {
        try (var storage = open()) {
            create(storage);
            storage.replace(stage(storage, "second"), DOCUMENT, hash(ORIGINAL));
            storage.replace(stage(storage, "third"), DOCUMENT, hash("second"));
            UUID corruptId = storage.listVersions(DOCUMENT).getFirst().id();
            Files.writeString(archive().resolve(corruptId.toString()).resolve("content"), "damage");
            Files.createDirectory(archive().resolve(UUID.randomUUID().toString()));
            assertEquals(1, storage.listVersions(DOCUMENT).size());
        }
        var publicationEntered = new CountDownLatch(1);
        var publish = new CountDownLatch(1);
        try (var storage = new FileStorageService(temporary.resolve("active"), 10_000, (source, target) -> {
            publicationEntered.countDown();
            try {
                if (!publish.await(5, TimeUnit.SECONDS)) {
                    throw new IOException("Timed out waiting for history read");
                }
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new IOException(exception);
            }
            Files.move(source, target, ATOMIC_MOVE, REPLACE_EXISTING);
        }); var tasks = Executors.newVirtualThreadPerTaskExecutor()) {
            Path source = stage(storage, "fourth");
            var modify = tasks.submit(() -> storage.replace(source, DOCUMENT, hash("third")));
            try {
                assertTrue(publicationEntered.await(5, TimeUnit.SECONDS));
                assertEquals(1, storage.listVersions(DOCUMENT).size());
            } finally {
                publish.countDown();
            }
            assertNotNull(modify.get(5, TimeUnit.SECONDS));
            assertEquals(2, storage.listVersions(DOCUMENT).size());
        }
    }

    @Test
    void historyProtocolValidatesNegotiationPathsAndEnvelope() throws Exception {
        try (var storage = open();
             var supported = new FileTransferService(storage, true, true, UUID.randomUUID(), (path, hash) -> {},
                 event -> {}, true, true);
             var legacy = new FileTransferService(storage, path -> {})) {
            byte[] missing = body(output -> writeText(output, "missing.txt"));
            UUID request = UUID.randomUUID();
            var requestFrame = new java.io.ByteArrayOutputStream();
            MessageCodec.write(new java.io.DataOutputStream(requestFrame),
                encode(VERSION_LIST_REQUEST, request, NONE, missing));
            assertEquals(19, requestFrame.toByteArray()[5]);
            var requestInput = new java.io.DataInputStream(new java.io.ByteArrayInputStream(requestFrame.toByteArray()));
            var response = decode(supported.handle(MessageCodec.read(requestInput)));
            assertEquals(VERSION_LIST_RESPONSE, response.type());
            assertEquals(request, response.requestId());
            assertEquals(NONE, response.transferId());
            var input = response.body();
            assertEquals(RESULT_OK, input.readUnsignedByte());
            assertTrue(readVersionList(input, "missing.txt").isEmpty());
            assertEquals(RESULT_NOT_IMPLEMENTED, decode(legacy.handle(
                encode(VERSION_LIST_REQUEST, request, NONE, missing))).body().readUnsignedByte());
            for (String invalid : List.of("../secret", "C:/secret", ".pending-secret", "nested/../secret")) {
                byte[] path = body(output -> writeText(output, invalid));
                assertEquals(RESULT_ERROR, decode(supported.handle(encode(VERSION_LIST_REQUEST, request, NONE, path)))
                    .body().readUnsignedByte());
            }
            assertThrows(ProtocolException.class,
                () -> supported.handle(encode(VERSION_LIST_REQUEST, request, NONE, new byte[]{0, 12, 1})));
            assertThrows(ProtocolException.class,
                () -> supported.handle(encode(VERSION_LIST_REQUEST, request, UUID.randomUUID(), missing)));
        }
    }

    @Test
    void historyCapsResultsAndFitsTheFrameAtMaximumPathLength() throws Exception {
        try (var storage = open()) {
            create(storage);
            String previous = ORIGINAL;
            for (int index = 0; index <= MAX_LIST_VERSIONS; index++) {
                String current = "edit " + index;
                assertNotNull(storage.replace(stage(storage, current), DOCUMENT, hash(previous)));
                previous = current;
            }
            var history = storage.listVersions(DOCUMENT);
            assertEquals(MAX_LIST_VERSIONS, history.size());
            assertEquals(HexFormat.of().formatHex(hash("edit 99")), history.getFirst().sha256());
            assertTrue(history.stream().noneMatch(version -> version.sha256().equals(HexFormat.of().formatHex(hash(ORIGINAL)))));
            String longPath = "a".repeat(255) + "/" + "b".repeat(254) + "/c";
            var largest = history.stream().map(version -> new FileVersion(version.id(), longPath, version.sha256(),
                version.size(), version.modifiedMillis(), version.archivedAt())).toList();
            byte[] result = versionListResult(largest);
            var message = encode(VERSION_LIST_RESPONSE, UUID.randomUUID(), NONE, result);
            var wire = new java.io.ByteArrayOutputStream();
            MessageCodec.write(new java.io.DataOutputStream(wire), message);
            assertTrue(wire.size() < MessageCodec.MAX_MESSAGE_BYTES);
            var frame = new java.io.DataInputStream(new java.io.ByteArrayInputStream(wire.toByteArray()));
            assertEquals(20, wire.toByteArray()[5]);
            var payload = decode(MessageCodec.read(frame)).body();
            assertEquals(RESULT_OK, payload.readUnsignedByte());
            assertEquals(largest, readVersionList(payload, longPath));
            var wrongPath = decode(message).body();
            assertEquals(RESULT_OK, wrongPath.readUnsignedByte());
            assertThrows(ProtocolException.class, () -> readVersionList(wrongPath, DOCUMENT));
            assertThrows(ProtocolException.class, () -> versionListResult(java.util.Collections.nCopies(
                MAX_LIST_VERSIONS + 1, history.getFirst())));
            var tooMany = new java.io.DataInputStream(new java.io.ByteArrayInputStream(
                body(output -> output.writeInt(MAX_LIST_VERSIONS + 1))));
            assertThrows(ProtocolException.class, () -> readVersionList(tooMany, DOCUMENT));
        }
    }

    private Path journal(boolean replaceActive) throws IOException {
        Path activePath = temporary.resolve("active");
        Path original;
        Path newFile;
        try (var storage = open()) {
            original = create(storage);
            newFile = stage(storage, "second");
        }
        try (var versions = new VersionStore(new SafeRoot(activePath))) {
            versions.prepare(DOCUMENT, original, SafeRoot.regularAttributes(original), hash(ORIGINAL), newFile);
            if (replaceActive) {
                Files.move(newFile, original, ATOMIC_MOVE, REPLACE_EXISTING);
            }
        }
        return original;
    }

    private Path race(FileStorageService storage, Path source, CountDownLatch ready, CountDownLatch start)
            throws IOException, InterruptedException {
        ready.countDown();
        start.await();
        return storage.replace(source, DOCUMENT, hash(ORIGINAL));
    }

    private FileStorageService open() throws IOException {
        return new FileStorageService(temporary.resolve("active"), 10_000);
    }

    private Path create(FileStorageService storage) throws IOException {
        return storage.commit(stage(storage, ORIGINAL), DOCUMENT);
    }

    private static Path stage(FileStorageService storage, String value) throws IOException {
        return Files.writeString(storage.temporaryUpload(), value);
    }

    private Path archive() {
        return temporary.resolve(".filesync-versions-active");
    }

    private List<Path> committed() throws IOException {
        try (var files = Files.list(archive())) {
            return files.filter(file -> !file.getFileName().toString().startsWith(".")).sorted().toList();
        }
    }

    private void assertNoJournal() throws IOException {
        try (var files = Files.list(archive())) {
            assertTrue(files.noneMatch(file -> file.getFileName().toString().startsWith(".stage-")
                || file.getFileName().toString().startsWith(".pending-")));
        }
    }

    private static Properties assertVersion(Path entry, String contents) throws IOException {
        var metadata = new Properties();
        try (var input = Files.newInputStream(entry.resolve("metadata.properties"))) {
            metadata.load(input);
        }
        var bytes = Files.readAllBytes(entry.resolve("content"));
        assertArrayEquals(contents.getBytes(StandardCharsets.UTF_8), bytes);
        assertEquals(Long.toString(bytes.length), metadata.getProperty("size"));
        assertEquals(HexFormat.of().formatHex(sha256().digest(bytes)), metadata.getProperty("sha256"));
        assertEquals(DOCUMENT, metadata.getProperty("path"));
        assertEquals(entry.getFileName().toString(), metadata.getProperty("id"));
        assertNotNull(UUID.fromString(metadata.getProperty("id")));
        return metadata;
    }

    private static String read(Path path) {
        try {
            return Files.readString(path);
        } catch (IOException exception) {
            throw new AssertionError(exception);
        }
    }

    private static byte[] hash(String value) {
        return sha256().digest(value.getBytes(StandardCharsets.UTF_8));
    }
}
