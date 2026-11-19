package com.dacs4.filesync.network;

import com.dacs4.filesync.shared.dto.FileMetadata;
import com.dacs4.filesync.shared.exception.ProtocolException;
import com.dacs4.filesync.shared.protocol.FileTransferCodec.Packet;
import com.dacs4.filesync.shared.protocol.MessageType;

import java.io.*;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.List;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import static com.dacs4.filesync.shared.protocol.FileTransferCodec.*;
import static com.dacs4.filesync.shared.protocol.MessageType.*;

public final class FileTransferClient {
    private static final byte[] EMPTY = new byte[0];
    private final TcpClient connection;

    public FileTransferClient(TcpClient connection) {
        this.connection = connection;
    }

    @FunctionalInterface
    public interface Progress {
        void update(long bytes, long total);
    }

    public static final class Failure extends IOException {
        public Failure(String message) {
            super(message);
        }
    }

    public static final class Cancelled extends IOException {}

    public List<FileMetadata> list() throws IOException {
        Packet reply = request(FILE_LIST_REQUEST, NONE, EMPTY);
        requireType(reply, FILE_LIST_RESPONSE);
        var input = reply.body();
        checkResult(input);
        return readList(input.readAllBytes());
    }

    public void upload(Path source, BooleanSupplier cancelled, Progress progress) throws IOException {
        String name = source.getFileName().toString();
        if (!validName(name)) {
            throw new Failure("Tên tệp không hợp lệ trên máy chủ.");
        }
        if (!Files.isRegularFile(source)) {
            throw new Failure("Hãy chọn một tệp thông thường để tải lên.");
        }
        long total = Files.size(source);
        UUID transfer = UUID.randomUUID();
        boolean opened = false;
        try (InputStream input = Files.newInputStream(source)) {
            checkCancelled(cancelled);
            Packet ready = request(UPLOAD_INIT, transfer, body(output -> {
                writeText(output, name);
                output.writeLong(total);
            }));
            checkReady(ready);
            opened = true;
            MessageDigest digest = sha256();
            progress.update(0, total);
            long sent = 0;
            while (sent < total) {
                checkCancelled(cancelled);
                int expected = (int) Math.min(CHUNK_BYTES, total - sent);
                byte[] bytes = input.readNBytes(expected);
                if (bytes.length != expected) {
                    throw new Failure("Tệp nguồn đã thay đổi kích thước trong khi tải lên.");
                }
                digest.update(bytes);
                checkReady(request(FILE_CHUNK, transfer, bytes));
                sent += bytes.length;
                progress.update(sent, total);
            }
            if (input.read() != -1) {
                throw new Failure("Tệp nguồn đã thay đổi kích thước trong khi tải lên.");
            }
            checkCancelled(cancelled);
            Packet result = request(UPLOAD_END, transfer, digest.digest());
            requireType(result, UPLOAD_RESULT);
            opened = false;
            checkOnlyResult(result);
        } finally {
            if (opened) {
                abort(UPLOAD_END, transfer, EMPTY);
            }
        }
    }

    public Path download(FileMetadata selected, Path directory, BooleanSupplier cancelled,
                         Progress progress) throws IOException {
        if (!validName(selected.name())) {
            throw new ProtocolException("Invalid download name");
        }
        checkCancelled(cancelled);
        Path root = Files.createDirectories(directory).toRealPath();
        Path target = root.resolve(selected.name());
        if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
            throw new FileAlreadyExistsException(target.toString());
        }
        UUID transfer = UUID.randomUUID();
        Path temporary = null;
        boolean opened = false;
        try {
            Packet info = request(DOWNLOAD_REQUEST, transfer,
                body(output -> writeText(output, selected.name())));
            requireType(info, DOWNLOAD_INFO);
            opened = true;
            var metadata = info.body();
            FileMetadata actual = readMetadata(metadata);
            requireEnd(metadata);
            if (!selected.name().equals(actual.name())) {
                throw new ProtocolException("Unexpected download filename");
            }

            temporary = Files.createTempFile(root, ".download-", ".part");
            receive(transfer, actual.size(), temporary, cancelled, progress);
            checkCancelled(cancelled);
            Packet result = request(DOWNLOAD_RESULT, transfer, new byte[]{0});
            requireType(result, DOWNLOAD_RESULT);
            opened = false;
            checkOnlyResult(result);
            checkCancelled(cancelled);
            Files.move(temporary, target);
            temporary = null;
            return target;
        } finally {
            try {
                if (temporary != null) {
                    Files.deleteIfExists(temporary);
                }
            } finally {
                if (opened) {
                    abort(DOWNLOAD_RESULT, transfer, new byte[]{1});
                }
            }
        }
    }

    private void receive(UUID transfer, long total, Path temporary, BooleanSupplier cancelled,
                         Progress progress) throws IOException {
        MessageDigest digest = sha256();
        long received = 0;
        progress.update(0, total);
        try (OutputStream output = Files.newOutputStream(temporary, LinkOption.NOFOLLOW_LINKS)) {
            while (received < total) {
                checkCancelled(cancelled);
                Packet packet = request(FILE_CHUNK, transfer, EMPTY);
                requireType(packet, FILE_CHUNK);
                byte[] bytes = packet.data();
                if (bytes.length != Math.min(CHUNK_BYTES, total - received)) {
                    throw new ProtocolException("Invalid download chunk length");
                }
                output.write(bytes);
                digest.update(bytes);
                received += bytes.length;
                progress.update(received, total);
            }
            checkCancelled(cancelled);
            Packet end = request(FILE_CHUNK, transfer, EMPTY);
            requireType(end, DOWNLOAD_END);
            if (!MessageDigest.isEqual(end.data(), digest.digest())) {
                throw new Failure("Mã SHA-256 không khớp. Tệp tải xuống chưa được lưu.");
            }
        }
    }

    private Packet request(MessageType type, UUID transfer, byte[] data) throws IOException {
        return connection.exchange(type, transfer, data);
    }

    private void abort(MessageType type, UUID transfer, byte[] data) {
        try {
            request(type, transfer, data);
        } catch (IOException ignored) {
            // A failed exchange closes the socket and the server discards any partial transfer.
        }
    }

    private static void checkReady(Packet packet) throws IOException {
        requireType(packet, UPLOAD_READY);
        requireEnd(packet.body());
    }

    private static void requireType(Packet packet, MessageType expected) throws IOException {
        if (packet.type() == expected) {
            return;
        }
        if (packet.type() == UPLOAD_RESULT || packet.type() == DOWNLOAD_RESULT) {
            checkOnlyResult(packet);
        }
        throw new ProtocolException("Unexpected transfer response type");
    }

    private static void checkOnlyResult(Packet packet) throws IOException {
        var input = packet.body();
        checkResult(input);
        requireEnd(input);
    }

    private static void checkResult(DataInputStream input) throws IOException {
        String error = readResult(input);
        if (error != null) {
            requireEnd(input);
            throw new Failure(error);
        }
    }

    private static void checkCancelled(BooleanSupplier cancelled) throws Cancelled {
        if (cancelled.getAsBoolean()) {
            throw new Cancelled();
        }
    }
}
