package com.fxmacrodata.kafka;

import com.fxmacrodata.client.FxMacroDataClient;
import com.fxmacrodata.client.Result;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.connect.errors.ConnectException;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

final class SnapshotCollectorTest {
    private static Result page(int index, boolean more) {
        return new Result("indicator_history", Map.of("data", List.of(Map.of("date", "synthetic-" + index, "value", index)),
            "pagination", Map.of("offset", index, "returned_count", 1, "total_count", 2, "next_offset", index + 1, "has_more", more),
            "dataset_version", "synthetic-version", "metadata", Map.of("opaque", List.of(1, 2))), "https://api.fxmacrodata.com/v1/announcements/usd/policy_rate");
    }
    @Test void offsetsCollectAllRecordsAndPreserveEveryPage() {
        AtomicInteger call = new AtomicInteger();
        Result result = SnapshotCollector.collect(FxMacroDataClient.operation("indicator_history"), Map.of("currency", "usd", "indicator", "policy_rate"), 2, 10, arguments -> {
            int index = call.getAndIncrement();
            assertEquals(index, ((Number) arguments.get("offset")).intValue());
            if (index == 1) assertEquals("synthetic-version", arguments.get("dataset_version"));
            return page(index, index == 0);
        });
        assertEquals(2, result.records().size());
        assertEquals(2, ((List<?>) ((Map<?, ?>) result.payload()).get("pages")).size());
        assertTrue(FxMacroDataClient.json(result.payload()).contains("opaque"));
    }
    @Test void budgetCannotSilentlyTruncateHistory() {
        assertThrows(ConnectException.class, () -> SnapshotCollector.collect(FxMacroDataClient.operation("indicator_history"), Map.of("currency", "usd", "indicator", "policy_rate"), 1, 10, args -> page(0, true)));
        AtomicInteger call = new AtomicInteger();
        assertThrows(ConnectException.class, () -> SnapshotCollector.collect(FxMacroDataClient.operation("indicator_history"), Map.of("currency", "usd", "indicator", "policy_rate"), 2, 1, args -> page(call.getAndIncrement(), call.get() == 1)));
    }
    @Test void calendarCursorPinsCheckpointAndKeepsVersionRecords() {
        AtomicInteger call = new AtomicInteger();
        Result result = SnapshotCollector.collect(FxMacroDataClient.operation("calendar_event_versions"), Map.of("currency", "usd", "event_id", "synthetic-event"), 3, 10, args -> {
            int index = call.getAndIncrement();
            java.util.Map<String, Object> payload = new java.util.LinkedHashMap<>();
            payload.put("checkpoint", 17); payload.put("next_cursor", index == 0 ? "synthetic-next" : null);
            payload.put("versions", List.of(Map.of("transition_id", "synthetic-" + index, "status", index == 0 ? "scheduled" : "cancelled")));
            if (index == 1) { assertEquals("synthetic-next", args.get("cursor")); assertEquals(17, args.get("checkpoint")); }
            return new Result("calendar_event_versions", payload, "https://api.fxmacrodata.com/v1/calendar/usd/events/synthetic-event/versions");
        });
        assertEquals(2, result.records().size());
        assertEquals("cancelled", result.records().get(1).get("status"));
    }
    @Test void mcpCalendarStructuredContentRetainsOriginalMcpEnvelope() {
        Map<String, Object> body = new java.util.LinkedHashMap<>();
        body.put("checkpoint", 9); body.put("next_cursor", null); body.put("events", List.of(Map.of("event_id", "synthetic", "status", "scheduled")));
        Result result = SnapshotCollector.collect(FxMacroDataClient.operation("mcp_calendar_snapshot"), Map.of("currency", "usd", "as_of", "2026-01-01T00:00:00Z"), 3, 10,
            args -> new Result("mcp_calendar_snapshot", Map.of("structuredContent", body, "content", List.of(Map.of("type", "text", "text", "synthetic content"))), "https://fxmacrodata.com"));
        assertEquals(1, result.records().size()); assertTrue(FxMacroDataClient.json(result.payload()).contains("synthetic content"));
    }
    @Test void changedCheckpointOrRepeatedCursorFailsClosed() {
        AtomicInteger count = new AtomicInteger();
        assertThrows(ConnectException.class, () -> SnapshotCollector.collect(FxMacroDataClient.operation("calendar_event_versions"), Map.of("currency", "usd", "event_id", "synthetic"), 3, 10, args ->
            new Result("calendar_event_versions", Map.of("checkpoint", count.incrementAndGet(), "next_cursor", "next", "versions", List.of(Map.of("id", count.get()))), "https://fxmacrodata.com")));
    }
    @Test void visualMcpOffsetsAreNotMistakenForExportPagination() {
        AtomicInteger count = new AtomicInteger();
        Result result = SnapshotCollector.collect(FxMacroDataClient.operation("mcp_indicator_visual_artifact"), Map.of(), 5, 10, args -> {
            count.incrementAndGet(); return new Result("mcp_indicator_visual_artifact", Map.of("artifact", "synthetic-svg"), "https://fxmacrodata.com");
        });
        assertEquals(1, count.get()); assertEquals("synthetic-svg", ((Map<?, ?>) result.payload()).get("artifact"));
    }
}
