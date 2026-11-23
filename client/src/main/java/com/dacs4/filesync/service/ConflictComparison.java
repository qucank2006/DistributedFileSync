package com.dacs4.filesync.service;

import com.dacs4.filesync.network.FileTransferClient;
import com.dacs4.filesync.shared.protocol.FileTransferCodec;
import com.dacs4.filesync.shared.protocol.FileTransferCodec.SafeRoot;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.Objects;
import java.util.function.BooleanSupplier;

/** Point-in-time previews captured off the UI thread under the existing file-operation lock. */
public final class ConflictComparison {
    public record Result(String localSha256, String serverSha256, LineDiff.Result diff) {}
    private record Snapshot(byte[] bytes, String hash, BasicFileAttributes attributes) {}

    private static final System.Logger LOG = System.getLogger(ConflictComparison.class.getName());

    private ConflictComparison() {}

    public static Result capture(FileTransferClient transfer, Path localRoot, String relative,
                                 BooleanSupplier cancelled, FileTransferClient.Progress progress) throws IOException {
        LineDiff.checkCancelled(cancelled);
        SafeRoot source;
        Snapshot local;
        try {
            source = SafeRoot.existing(localRoot);
            local = read(source.resolve(relative, false), "cục bộ", cancelled);
        } catch (NoSuchFileException exception) {
            throw new IOException("Không tìm thấy tệp cục bộ.", exception);
        }
        var server = transfer.serverVersion(relative)
            .orElseThrow(() -> new IOException("Không tìm thấy tệp trên máy chủ."));
        if (server.metadata().size() > LineDiff.MAX_BYTES) {
            throw LineDiff.sizeLimit("máy chủ");
        }
        LineDiff.checkCancelled(cancelled);

        Path temporaryRoot = SafeRoot.existing(Path.of(System.getProperty("java.io.tmpdir"))).path();
        if (temporaryRoot.startsWith(source.path())) {
            throw new IOException("Thư mục tạm phải nằm ngoài thư mục đồng bộ để so sánh nội dung.");
        }
        Path scratch = Files.createTempDirectory(temporaryRoot, "dfs-comparison-");
        try {
            Path downloaded;
            try {
                downloaded = transfer.downloadSnapshot(server, SafeRoot.existing(scratch), cancelled, progress);
            } catch (FileTransferClient.Failure exception) {
                if (exception.resultCode() == FileTransferCodec.RESULT_CONFLICT) {
                    throw stale("máy chủ");
                }
                throw exception;
            }
            var remote = read(downloaded, "máy chủ", cancelled);
            if (!server.sha256().equals(remote.hash())) {
                throw stale("máy chủ");
            }
            var diff = LineDiff.compare(local.bytes(), remote.bytes(), cancelled);
            LineDiff.checkCancelled(cancelled);
            if (transfer.serverVersion(relative).filter(server::equals).isEmpty()) {
                throw stale("máy chủ");
            }
            Snapshot checked;
            try {
                checked = read(source.resolve(relative, false), "cục bộ", cancelled);
            } catch (NoSuchFileException exception) {
                throw stale("cục bộ");
            }
            if (!local.hash().equals(checked.hash()) || !sameFile(local.attributes(), checked.attributes())) {
                throw stale("cục bộ");
            }
            return new Result(local.hash(), remote.hash(), diff);
        } finally {
            cleanup(scratch);
        }
    }

    private static Snapshot read(Path path, String side, BooleanSupplier cancelled) throws IOException {
        LineDiff.checkCancelled(cancelled);
        var before = SafeRoot.regularAttributes(path);
        if (before.size() > LineDiff.MAX_BYTES) {
            throw LineDiff.sizeLimit(side);
        }
        byte[] bytes;
        try (var input = Files.newInputStream(path, LinkOption.NOFOLLOW_LINKS)) {
            bytes = input.readNBytes(LineDiff.MAX_BYTES + 1);
        }
        LineDiff.checkCancelled(cancelled);
        if (bytes.length != before.size() || !sameFile(before, SafeRoot.regularAttributes(path))) {
            throw stale(side);
        }
        return new Snapshot(bytes, HexFormat.of().formatHex(FileTransferCodec.sha256().digest(bytes)), before);
    }

    private static boolean sameFile(BasicFileAttributes a, BasicFileAttributes b) {
        return a.size() == b.size() && a.lastModifiedTime().equals(b.lastModifiedTime())
            && a.creationTime().equals(b.creationTime()) && Objects.equals(a.fileKey(), b.fileKey());
    }

    private static IOException stale(String side) {
        return new IOException("Tệp " + side + " đã thay đổi khi chuẩn bị so sánh. Hãy mở lại bản so sánh.");
    }

    private static void cleanup(Path scratch) {
        try (var paths = Files.walk(scratch)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        } catch (IOException | java.io.UncheckedIOException exception) {
            LOG.log(System.Logger.Level.WARNING, "Could not remove comparison preview files", exception);
        }
    }
}
