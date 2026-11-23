package com.dacs4.filesync.sync;

import com.dacs4.filesync.network.FileTransferClient;
import com.dacs4.filesync.service.LineDiff;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static com.dacs4.filesync.service.LineDiff.Change.*;
import static org.junit.jupiter.api.Assertions.*;

class LineDiffTest {
    @Test
    void identicalAndEmptyFilesHaveNoDifferences() throws Exception {
        assertTrue(diff("", "").rows().isEmpty());
        assertTrue(diff("xin chào\n", "xin chào\n").differences().isEmpty());
        assertEquals(ADDED, diff("", "new").rows().getFirst().change());
        assertEquals(REMOVED, diff("old", "").rows().getFirst().change());
    }

    @Test
    void groupsSingleAndMultipleChangesWithAlignedLineNumbers() throws Exception {
        assertEquals(List.of(1), diff("a\nold\nb", "a\nnew\nb").differences());
        var changed = diff("same\nold\nkeep\nold two", "same\nnew\nkeep\nnew two");
        assertEquals(List.of(1, 3), changed.differences());
        assertEquals(List.of(SAME, MODIFIED, SAME, MODIFIED), changed.rows().stream().map(LineDiff.Row::change).toList());
        assertEquals(new LineDiff.Row(4, "old two", 4, "new two", MODIFIED), changed.rows().getLast());
    }

    @Test
    void alignsAddedAndRemovedLinesAroundUnchangedAnchors() throws Exception {
        var result = diff("anchor\nremove\nlast", "insert\nanchor\nlast");
        assertEquals(new LineDiff.Row(0, "", 1, "insert", ADDED), result.rows().getFirst());
        assertEquals(new LineDiff.Row(2, "remove", 0, "", REMOVED), result.rows().get(2));
        assertEquals(List.of(0, 2), result.differences());
    }

    @Test
    void normalizesLineEndingsAndHandlesLongLines() throws Exception {
        assertTrue(diff("\uFEFFa\r\nb\rc\n", "a\nb\nc").differences().isEmpty());
        String longLine = "a".repeat(LineDiff.MAX_LINE_LENGTH - 1);
        assertEquals(longLine + "b", diff(longLine + "b", longLine + "c").rows().getFirst().local());
        assertThrows(IOException.class, () -> diff(longLine + "bc", ""));
    }

    @Test
    void rejectsBinaryAndInvalidUtf8() {
        for (byte[] bytes : List.of(new byte[]{0, 12, 5}, new byte[]{(byte) 0xc3, 0x28})) {
            var failure = assertThrows(IOException.class, () -> LineDiff.compare(bytes, new byte[0], () -> false));
            assertTrue(failure.getMessage().contains("UTF-8"));
        }
    }

    @Test
    void enforcesByteAndLineBudgetsAndCooperativeCancellation() {
        assertThrows(IOException.class, () -> diff("x".repeat(LineDiff.MAX_BYTES + 1), ""));
        assertThrows(IOException.class, () -> diff("x\n".repeat(LineDiff.MAX_LINES + 1), ""));
        assertThrows(FileTransferClient.Cancelled.class,
            () -> LineDiff.compare(new byte[]{'a'}, new byte[]{'b'}, () -> true));
    }

    private static LineDiff.Result diff(String left, String right) throws IOException {
        return LineDiff.compare(left.getBytes(StandardCharsets.UTF_8), right.getBytes(StandardCharsets.UTF_8),
            () -> false);
    }
}
