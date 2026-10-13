package com.fxmacrodata.kafka;

import com.fxmacrodata.client.FxMacroDataClient;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

record StreamSpec(String operation, Map<String, Object> arguments, String topic) {
    static StreamSpec parse(Object entry) {
        if (!(entry instanceof Map<?, ?> map) || !Set.of("operation", "arguments", "topic").containsAll(map.keySet())) throw new IllegalArgumentException();
        if (!(map.get("operation") instanceof String operation) || !(map.get("topic") instanceof String topic)
                || !topic.matches("[A-Za-z0-9._-]{1,249}") || topic.equals(".") || topic.equals("..")) throw new IllegalArgumentException();
        if (operation.startsWith("mcp:")) operation = "mcp_" + operation.substring(4);
        Object input = map.get("arguments");
        Map<String, Object> args = new LinkedHashMap<>();
        if (input != null) {
            if (!(input instanceof Map<?, ?> raw)) throw new IllegalArgumentException();
            raw.forEach((key, value) -> args.put(String.valueOf(key), value));
        }
        rejectSecrets(args);
        FxMacroDataClient.validate(operation, args);
        return new StreamSpec(operation, java.util.Collections.unmodifiableMap(new LinkedHashMap<>(args)), topic);
    }

    private static void rejectSecrets(Object value) {
        if (value instanceof Map<?, ?> map) map.forEach((key, item) -> {
            String normalized = String.valueOf(key).replace("_", "").replace("-", "").toLowerCase();
            if (Set.of("apikey", "accesstoken", "authorization", "password", "secret", "token", "clientsecret", "url", "baseurl").contains(normalized)) throw new IllegalArgumentException();
            rejectSecrets(item);
        });
        else if (value instanceof Iterable<?> items) items.forEach(StreamSpec::rejectSecrets);
    }

    Map<String, Object> asMap() { return Map.of("operation", operation, "arguments", arguments, "topic", topic); }
    String id() { return digest(FxMacroDataClient.json(canonical(asMap()))); }
    Map<String, String> partition() { return Map.of("stream", id(), "operation", operation, "topic", topic); }

    static Object canonical(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> sorted = new TreeMap<>();
            map.forEach((key, item) -> sorted.put(String.valueOf(key), canonical(item)));
            return sorted;
        }
        if (value instanceof java.util.List<?> list) return list.stream().map(StreamSpec::canonical).toList();
        return value;
    }

    static String digest(String value) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8))); }
        catch (java.security.NoSuchAlgorithmException impossible) { throw new IllegalStateException("SHA-256 is unavailable."); }
    }
}
