package com.dacs4.filesync.service;

import com.dacs4.filesync.network.FileTransferClient.Cancelled;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.BooleanSupplier;

public final class LineDiff {
    public static final int MAX_BYTES = 512 * 1024;
    public static final int MAX_LINES = 2000;
    public static final int MAX_LINE_LENGTH = 8192;

    public enum Change { SAME, ADDED, REMOVED, MODIFIED }
    public record Row(int localNumber, String local, int serverNumber, String server, Change change) {}
    public record Result(List<Row> rows, List<Integer> differences) {}

    private LineDiff() {}

    public static Result compare(byte[] local, byte[] server, BooleanSupplier cancelled) throws IOException {
        List<String> left = lines(local, "cục bộ");
        List<String> right = lines(server, "máy chủ");
        var ids = new HashMap<String, Integer>();
        int[] a = left.stream().mapToInt(line -> ids.computeIfAbsent(line, key -> ids.size())).toArray();
        int[] b = right.stream().mapToInt(line -> ids.computeIfAbsent(line, key -> ids.size())).toArray();

        // The LCS length fits in a short. Match integer line IDs inside the quadratic loop.
        int stride = b.length + 1;
        short[] lcs = new short[(a.length + 1) * stride];
        for (int i = a.length - 1; i >= 0; i--) {
            checkCancelled(cancelled);
            for (int j = b.length - 1; j >= 0; j--) {
                int k = i * stride + j;
                lcs[k] = (short) (a[i] == b[j] ? 1 + lcs[k + stride + 1]
                    : Math.max(lcs[k + stride], lcs[k + 1]));
            }
        }

        var rows = new ArrayList<Row>();
        var differences = new ArrayList<Integer>();
        int i = 0;
        int j = 0;
        while (i < a.length || j < b.length) {
            checkCancelled(cancelled);
            if (i < a.length && j < b.length && a[i] == b[j]) {
                rows.add(new Row(i + 1, left.get(i), j + 1, right.get(j), Change.SAME));
                i++;
                j++;
                continue;
            }
            int firstLeft = i;
            int firstRight = j;
            while (i < a.length || j < b.length) {
                if (i < a.length && j < b.length && a[i] == b[j]) {
                    break;
                }
                if (j == b.length || (i < a.length && lcs[(i + 1) * stride + j] >= lcs[i * stride + j + 1])) {
                    i++;
                } else {
                    j++;
                }
            }
            differences.add(rows.size());
            for (int offset = 0; offset < Math.max(i - firstLeft, j - firstRight); offset++) {
                int l = firstLeft + offset;
                int r = firstRight + offset;
                boolean hasLeft = l < i;
                boolean hasRight = r < j;
                rows.add(new Row(hasLeft ? l + 1 : 0, hasLeft ? left.get(l) : "",
                    hasRight ? r + 1 : 0, hasRight ? right.get(r) : "",
                    !hasLeft ? Change.ADDED : !hasRight ? Change.REMOVED : Change.MODIFIED));
            }
        }
        return new Result(List.copyOf(rows), List.copyOf(differences));
    }

    private static List<String> lines(byte[] bytes, String side) throws IOException {
        if (bytes.length > MAX_BYTES) {
            throw sizeLimit(side);
        }
        String text;
        try {
            text = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException exception) {
            throw unsupported(side);
        }
        if (text.codePoints().anyMatch(c -> Character.isISOControl(c) && c != '\t' && c != '\r' && c != '\n')) {
            throw unsupported(side);
        }
        if (text.startsWith("\uFEFF")) {
            text = text.substring(1);
        }
        var lines = text.lines().limit(MAX_LINES + 1L).toList();
        if (lines.size() > MAX_LINES || lines.stream().anyMatch(line -> line.length() > MAX_LINE_LENGTH)) {
            throw new IOException("Tệp " + side + " vượt giới hạn 2.000 dòng hoặc 8.192 ký tự mỗi dòng.");
        }
        return lines;
    }

    static IOException sizeLimit(String side) {
        return new IOException("Tệp " + side + " vượt giới hạn xem trước 512 KiB.");
    }

    private static IOException unsupported(String side) {
        return new IOException("Không hỗ trợ nội dung tệp " + side + ". Chỉ xem được văn bản UTF-8 hợp lệ.");
    }

    static void checkCancelled(BooleanSupplier cancelled) throws Cancelled {
        if (cancelled.getAsBoolean()) {
            throw new Cancelled();
        }
    }
}
