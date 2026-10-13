package com.fxmacrodata.kafka;

import com.fxmacrodata.client.FxMacroDataClient;
import com.fxmacrodata.client.Operation;
import com.fxmacrodata.client.Result;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.kafka.connect.errors.ConnectException;

/** Lossless public-response pagination with explicit budgets and pinned versions. */
final class SnapshotCollector {
    static Result collect(FxMacroDataClient client, StreamSpec stream, int maxPages, int maxRecords, int maxBytes) {
        return collect(FxMacroDataClient.operation(stream.operation()), stream.arguments(), maxPages, maxRecords, maxBytes,
            args -> client.execute(stream.operation(), args));
    }

    static Result collect(Operation operation, Map<String, Object> arguments, int maxPages, int maxRecords,
                          Function<Map<String, Object>, Result> fetch) {
        return collect(operation, arguments, maxPages, maxRecords, 67108864, fetch);
    }

    static Result collect(Operation operation, Map<String, Object> arguments, int maxPages, int maxRecords, int maxBytes,
                          Function<Map<String, Object>, Result> fetch) {
        Object rawProperties = operation.inputSchema().get("properties");
        Map<?, ?> properties = rawProperties instanceof Map<?, ?> map ? map : Map.of();
        boolean offsets = !operation.method().equals("MCP") && properties.containsKey("offset");
        boolean cursors = properties.containsKey("cursor") && (!operation.method().equals("MCP") || Set.of("mcp_calendar_snapshot", "mcp_calendar_event_versions").contains(operation.name()));
        if (!offsets && !cursors) return fetch.apply(arguments);
        if (arguments.containsKey("page") || integer(arguments.getOrDefault("offset", 0)) != 0
                || arguments.get("cursor") != null) throw new ConnectException("Complete snapshot must start at the first page.");
        Map<String, Object> query = new LinkedHashMap<>(arguments);
        if (properties.containsKey("limit")) query.putIfAbsent("limit", 100);
        List<Object> records = new ArrayList<>();
        List<Object> pages = new ArrayList<>();
        Set<String> seenCursors = new LinkedHashSet<>();
        Set<String> seenPages = new LinkedHashSet<>();
        long offset = 0;
        long consumedBytes = 0;
        Long total = null;
        Object version = null;
        Object checkpoint = null;
        boolean first = true;
        String source = FxMacroDataClient.WEBSITE;
        for (int page = 0; page < maxPages; page++) {
            if (offsets) query.put("offset", offset);
            Result result = fetch.apply(java.util.Collections.unmodifiableMap(new LinkedHashMap<>(query)));
            source = result.sourceUrl();
            consumedBytes += FxMacroDataClient.json(result.payload()).getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
            if (consumedBytes > maxBytes) throw new ConnectException("FXMacroData collected pages exceed max.snapshot.bytes.");
            Map<?, ?> payload = publicPayload(result.payload());
            if ("source".equals(query.get("value_mode")) && (!"source".equals(payload.get("value_mode"))
                    || !(payload.get("value_metadata") instanceof Map<?, ?> metadata)
                    || !Boolean.FALSE.equals(metadata.get("normalization_applied"))))
                throw new ConnectException("FXMacroData snapshot did not confirm requested source-unit values.");
            String field = cursors ? (operation.name().endsWith("event_versions") ? "versions" : "events") : "data";
            if (!(payload.get(field) instanceof List<?> rows)) throw new ConnectException("FXMacroData pagination omitted its record array.");
            if (records.size() + rows.size() > maxRecords) throw new ConnectException("FXMacroData snapshot exceeds max.records.");
            Object currentVersion = payload.get("dataset_version");
            if (currentVersion == null && payload.get("replay") instanceof Map<?, ?> replay) currentVersion = replay.get("dataset_version");
            if (!first && version != null && !version.equals(currentVersion)) throw new ConnectException("FXMacroData content version changed during pagination.");
            if (currentVersion != null) {
                version = currentVersion;
                if (properties.containsKey("dataset_version")) query.put("dataset_version", version);
            }
            records.addAll(rows); pages.add(result.payload());
            if (!rows.isEmpty() && !seenPages.add(FxMacroDataClient.json(StreamSpec.canonical(rows))))
                throw new ConnectException("FXMacroData repeated a pagination page.");
            boolean more;
            if (cursors) {
                Object currentCheckpoint = payload.get("checkpoint");
                if (currentCheckpoint == null || !first && !java.util.Objects.equals(checkpoint, currentCheckpoint))
                    throw new ConnectException("FXMacroData cursor checkpoint changed or disappeared.");
                checkpoint = currentCheckpoint;
                if (!payload.containsKey("next_cursor")) throw new ConnectException("FXMacroData cursor response omitted continuation metadata.");
                Object next = payload.get("next_cursor");
                more = next != null;
                if (more) {
                    if (!(next instanceof String cursor) || cursor.isEmpty() || rows.isEmpty() || !seenCursors.add(cursor))
                        throw new ConnectException("FXMacroData pagination cursor did not advance.");
                    query.put("cursor", cursor);
                    if (properties.containsKey("checkpoint")) query.put("checkpoint", checkpoint);
                }
            } else {
                if (!(payload.get("pagination") instanceof Map<?, ?> pagination) || !(pagination.get("has_more") instanceof Boolean hasMore))
                    throw new ConnectException("FXMacroData response omitted pagination metadata.");
                long returned = pagination.get("returned_count") == null ? rows.size() : integer(pagination.get("returned_count"));
                long actual = pagination.get("offset") == null ? offset : integer(pagination.get("offset"));
                Long nextTotal = pagination.get("total_count") == null ? null : integer(pagination.get("total_count"));
                if (returned != rows.size() || actual != offset || nextTotal != null && nextTotal < offset + returned
                        || !first && total != null && !total.equals(nextTotal)) throw new ConnectException("FXMacroData returned inconsistent pagination metadata.");
                total = nextTotal; offset += returned; more = hasMore;
                if (more && (returned == 0 || pagination.get("next_offset") == null || integer(pagination.get("next_offset")) != offset))
                    throw new ConnectException("FXMacroData offset pagination did not advance contiguously.");
                if (!more && total != null && total != offset) throw new ConnectException("FXMacroData snapshot ended before its advertised total.");
            }
            first = false;
            if (!more) return new Result(operation.name(), Map.of("data", records, "pages", pages, "complete", true,
                "rows_returned", records.size(), "pages_fetched", page + 1), source);
        }
        throw new ConnectException("FXMacroData snapshot exhausted max.pages before completion.");
    }

    private static Map<?, ?> publicPayload(Object value) {
        if (!(value instanceof Map<?, ?> map)) throw new ConnectException("FXMacroData pagination response is not an object.");
        if (map.get("structuredContent") instanceof Map<?, ?> structured) return publicPayload(structured);
        if (map.get("result") instanceof Map<?, ?> result) return publicPayload(result);
        if (map.get("content") instanceof List<?> content) {
            for (Object block : content) if (block instanceof Map<?, ?> item && "text".equals(item.get("type")) && item.get("text") instanceof String text) {
                try { return publicPayload(FxMacroDataClient.parse(text)); }
                catch (RuntimeException ignored) { }
            }
        }
        return map;
    }

    private static long integer(Object value) {
        try { return new BigDecimal(value.toString()).longValueExact(); }
        catch (RuntimeException error) { throw new ConnectException("FXMacroData pagination contains an invalid integer."); }
    }
}
