package com.fxmacrodata.client;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Original response, additive record view and credential-free source provenance. */
public record Result(String operation, Object payload, String sourceUrl) {
    public Result {
        payload = FxMacroDataClient.immutable(payload);
    }

    public List<Map<String, Object>> records() {
        return recordsOf(payload);
    }

    public Map<String, Object> asMap() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("operation", operation);
        result.put("data", payload);
        result.put("records", records());
        result.put("source_url", sourceUrl);
        return FxMacroDataClient.immutableMap(result);
    }

    private static List<Map<String, Object>> recordsOf(Object value) {
        if (value == null) {
            return List.of();
        }
        if (value instanceof List<?> list) {
            List<Map<String, Object>> rows = new ArrayList<>();
            for (Object item : list) {
                Map<String, Object> row = item instanceof Map<?, ?> map ? stringMap(map) : singleton("value", item);
                rows.add(FxMacroDataClient.immutableMap(row));
            }
            return List.copyOf(rows);
        }
        if (!(value instanceof Map<?, ?> raw)) {
            return List.of(FxMacroDataClient.immutableMap(singleton("value", value)));
        }
        Map<String, Object> map = stringMap(raw);
        for (String key : List.of("structuredContent", "result", "data", "rows", "events", "announcements", "predictions", "prices", "commodities", "results", "series", "indicators")) {
            Object nested = map.get(key);
            if (nested instanceof Map<?, ?> nestedMap && !nestedMap.isEmpty()
                    && nestedMap.values().stream().allMatch(item -> item instanceof Map<?, ?>)) {
                List<Map<String, Object>> rows = new ArrayList<>();
                nestedMap.forEach((recordKey, record) -> rows.add(FxMacroDataClient.immutableMap(Map.of("record_key", recordKey, "record", record))));
                return List.copyOf(rows);
            }
            if (nested instanceof Map<?, ?> || nested instanceof List<?>) {
                return recordsOf(nested);
            }
        }
        if (map.get("content") instanceof List<?> content) {
            List<Map<String, Object>> rows = new ArrayList<>();
            for (Object item : content) {
                if (item instanceof Map<?, ?> block) {
                    if ("text".equals(block.get("type"))) {
                        String text = String.valueOf(block.getOrDefault("text", null));
                        try {
                            rows.addAll(recordsOf(FxMacroDataClient.parse(text)));
                        } catch (FxMacroDataException ignored) {
                            rows.add(FxMacroDataClient.immutableMap(Map.of("text", text)));
                        }
                    } else {
                        rows.add(FxMacroDataClient.immutableMap(stringMap(block)));
                    }
                }
            }
            return List.copyOf(rows);
        }
        return map.isEmpty() ? List.of() : List.of(FxMacroDataClient.immutableMap(map));
    }

    private static Map<String, Object> singleton(String key, Object value) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put(key, value);
        return result;
    }

    private static Map<String, Object> stringMap(Map<?, ?> raw) {
        Map<String, Object> result = new LinkedHashMap<>();
        raw.forEach((key, value) -> result.put(String.valueOf(key), value));
        return result;
    }
}
