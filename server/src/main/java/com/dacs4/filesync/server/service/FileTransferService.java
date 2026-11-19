package com.dacs4.filesync.server.service;

import com.dacs4.filesync.shared.dto.FileMetadata;
import com.dacs4.filesync.shared.exception.ProtocolException;
import com.dacs4.filesync.shared.protocol.MessageType;
import com.dacs4.filesync.shared.protocol.ProtocolMessage;

import java.io.*;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.dacs4.filesync.shared.protocol.FileTransferCodec.*;
import static com.dacs4.filesync.shared.protocol.MessageType.*;

/** One transfer at a time for this session. Only the session reader calls this service. */
public final class FileTransferService implements AutoCloseable {
    private static final System.Logger LOG = System.getLogger(FileTransferService.class.getName());
    private static final long IDLE_NANOS = TimeUnit.SECONDS.toNanos(30);
    private static final byte[] EMPTY = new byte[0];

    private final FileStorageService storage;
    private UUID transfer;
    private long lastActivity;
    private long remaining;
    private boolean downloadEnded;
    private MessageDigest digest;
    private Path temporary;
    private Path target;
    private OutputStream upload;
    private InputStream download;

    public FileTransferService(FileStorageService storage) {
        this.storage = storage;
    }

    public ProtocolMessage handle(ProtocolMessage message) throws IOException {
        Packet packet = decode(message);
        expireIfIdle();
        try {
            return switch (packet.type()) {
                case UPLOAD_INIT -> beginUpload(packet);
                case FILE_CHUNK -> chunk(packet);
                case UPLOAD_END -> endUpload(packet);
                case DOWNLOAD_REQUEST -> beginDownload(packet);
                case DOWNLOAD_RESULT -> endDownload(packet);
                case FILE_LIST_REQUEST -> list(packet);
                default -> throw new ProtocolException("Unexpected client transfer message");
            };
        } catch (ProtocolException exception) {
            throw exception;
        } catch (EOFException | java.nio.charset.CharacterCodingException exception) {
            throw new ProtocolException("Malformed transfer payload", exception);
        } catch (IOException exception) {
            if (packet.transferId().equals(transfer)) {
                close();
            }
            MessageType responseType = switch (packet.type()) {
                case FILE_LIST_REQUEST -> FILE_LIST_RESPONSE;
                case DOWNLOAD_REQUEST, DOWNLOAD_RESULT -> DOWNLOAD_RESULT;
                case FILE_CHUNK -> packet.data().length == 0 ? DOWNLOAD_RESULT : UPLOAD_RESULT;
                default -> UPLOAD_RESULT;
            };
            String error = switch (exception) {
                case FileAlreadyExistsException ignored -> "Tệp cùng tên đã tồn tại trên máy chủ.";
                case NoSuchFileException ignored -> "Không tìm thấy tệp trên máy chủ.";
                case TransferFailure failure -> failure.getMessage();
                default -> "Máy chủ không thể đọc hoặc lưu tệp.";
            };
            return reply(packet, responseType, result(error));
        }
    }

    private ProtocolMessage beginUpload(Packet packet) throws IOException {
        var input = packet.body();
        String name = readText(input);
        long size = input.readLong();
        requireEnd(input);
        if (!validName(name) || size < 0 || size > storage.maxFileBytes()) {
            throw new TransferFailure("Tên hoặc dung lượng tệp không hợp lệ. Giới hạn: " + storage.maxFileBytes() + " byte.");
        }
        begin(packet, size);
        target = storage.file(name);
        if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
            throw new FileAlreadyExistsException(name);
        }
        temporary = storage.temporaryUpload();
        upload = Files.newOutputStream(temporary, LinkOption.NOFOLLOW_LINKS);
        return reply(packet, UPLOAD_READY, EMPTY);
    }

    private ProtocolMessage beginDownload(Packet packet) throws IOException {
        var input = packet.body();
        String name = readText(input);
        requireEnd(input);
        if (!validName(name)) {
            throw new TransferFailure("Tên tệp không hợp lệ.");
        }
        Path path = storage.file(name);
        FileMetadata metadata = storage.metadata(path);
        if (metadata.size() > storage.maxFileBytes()) {
            throw new TransferFailure("Tệp vượt giới hạn tải xuống.");
        }
        begin(packet, metadata.size());
        download = Files.newInputStream(path, LinkOption.NOFOLLOW_LINKS);
        return reply(packet, DOWNLOAD_INFO, body(output -> writeMetadata(output, metadata)));
    }

    private void begin(Packet packet, long size) throws IOException {
        if (transfer != null) {
            if (transfer.equals(packet.transferId())) {
                throw new ProtocolException("Duplicate transfer initialization");
            }
            throw new TransferFailure("Phiên này đang truyền một tệp khác.");
        }
        transfer = packet.transferId();
        remaining = size;
        digest = sha256();
        lastActivity = System.nanoTime();
    }

    private ProtocolMessage chunk(Packet packet) throws IOException {
        requireTransfer(packet);
        byte[] data = packet.data();
        if (upload != null) {
            if (data.length == 0 || data.length > CHUNK_BYTES || data.length > remaining) {
                throw new ProtocolException("Invalid upload chunk size");
            }
            upload.write(data);
            digest.update(data);
            remaining -= data.length;
            return reply(packet, UPLOAD_READY, EMPTY);
        }
        if (download == null || data.length != 0 || downloadEnded) {
            throw new ProtocolException("Unexpected download pull");
        }
        if (remaining == 0) {
            if (download.read() != -1) {
                throw new TransferFailure("Tệp trên máy chủ đã thay đổi trong khi tải xuống.");
            }
            downloadEnded = true;
            return reply(packet, DOWNLOAD_END, digest.digest());
        }
        int expected = (int) Math.min(CHUNK_BYTES, remaining);
        byte[] bytes = download.readNBytes(expected);
        if (bytes.length != expected) {
            throw new TransferFailure("Tệp trên máy chủ đã thay đổi trong khi tải xuống.");
        }
        digest.update(bytes);
        remaining -= bytes.length;
        return reply(packet, FILE_CHUNK, bytes);
    }

    private ProtocolMessage endUpload(Packet packet) throws IOException {
        requireTransfer(packet);
        byte[] hash = packet.data();
        if (upload == null || (hash.length != 0 && hash.length != HASH_BYTES)) {
            throw new ProtocolException("Invalid upload end");
        }
        if (hash.length != 0) {
            if (remaining != 0 || !MessageDigest.isEqual(hash, digest.digest())) {
                throw new TransferFailure("Dung lượng hoặc mã SHA-256 của tệp tải lên không khớp.");
            }
            upload.close();
            upload = null;
            storage.commit(temporary, target);
            temporary = null;
        }
        close();
        return reply(packet, UPLOAD_RESULT, result(null));
    }

    private ProtocolMessage endDownload(Packet packet) throws IOException {
        requireTransfer(packet);
        byte[] data = packet.data();
        if (download == null || data.length != 1 || (data[0] != 0 && data[0] != 1)
                || (data[0] == 0 && !downloadEnded)) {
            throw new ProtocolException("Invalid download result");
        }
        close();
        return reply(packet, DOWNLOAD_RESULT, result(null));
    }

    private ProtocolMessage list(Packet packet) throws IOException {
        if (packet.data().length != 0) {
            throw new ProtocolException("Invalid list request");
        }
        var files = storage.list();
        return reply(packet, FILE_LIST_RESPONSE, body(output -> {
            output.writeByte(0);
            output.writeInt(files.size());
            for (var file : files) {
                writeMetadata(output, file);
            }
        }));
    }

    private void requireTransfer(Packet packet) throws IOException {
        if (!packet.transferId().equals(transfer)) {
            throw new TransferFailure("Phiên truyền tệp đã hết hạn hoặc không tồn tại.");
        }
        lastActivity = System.nanoTime();
    }

    public void expireIfIdle() {
        if (transfer != null && System.nanoTime() - lastActivity >= IDLE_NANOS) {
            close();
        }
    }

    private static ProtocolMessage reply(Packet packet, MessageType type, byte[] data) {
        return encode(type, packet.requestId(), packet.transferId(), data);
    }

    @Override
    public void close() {
        try (InputStream input = download; OutputStream output = upload) {
            // Resource closure precedes deletion on Windows.
        } catch (IOException exception) {
            LOG.log(System.Logger.Level.WARNING, "Could not close transfer stream", exception);
        } finally {
            if (temporary != null) {
                try {
                    Files.deleteIfExists(temporary);
                } catch (IOException exception) {
                    LOG.log(System.Logger.Level.WARNING, "Could not delete incomplete upload", exception);
                }
            }
            temporary = null;
            upload = null;
            download = null;
            transfer = null;
            downloadEnded = false;
        }
    }

    private static final class TransferFailure extends IOException {
        TransferFailure(String message) {
            super(message);
        }
    }
}
