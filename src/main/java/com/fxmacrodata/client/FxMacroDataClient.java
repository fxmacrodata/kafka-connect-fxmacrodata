package com.fxmacrodata.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SpecificationVersion;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

/** Fixed-origin public REST/MCP transport. No telemetry or backend implementation. */
public final class FxMacroDataClient implements AutoCloseable {
    public static final String API_ORIGIN = "https://api.fxmacrodata.com";
    public static final String MCP_URL = "https://mcp.fxmacrodata.com/mcp";
    public static final String WEBSITE = "https://fxmacrodata.com";
    public static final int MAX_RESPONSE_BYTES = 10 * 1024 * 1024;
    private static final String PROTOCOL = "2025-03-26";
    private static final ObjectMapper JSON = new ObjectMapper()
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    private static final Map<String, Operation> OPERATIONS = loadOperations();
    private static final SchemaRegistry SCHEMAS = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12);
    private static final Set<String> SECRET_FIELDS = Set.of("apikey", "accesstoken", "authorization", "proxyauthorization", "password", "clientsecret", "secret", "token");
    private final String apiKey;
    private final double timeout;
    private final Transport transport;
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(task -> {
        Thread thread = new Thread(task, "fxmacrodata-read-deadline");
        thread.setDaemon(true);
        return thread;
    });
    private Map<String, String> mcpHeaders;
    private long requestId;
    private boolean closed;

    public FxMacroDataClient(String apiKey, double timeout) {
        this(apiKey, timeout, new JdkTransport(timeout));
    }

    /** Supply an application-owned transport for deterministic tests or HTTP instrumentation. */
    public FxMacroDataClient(String apiKey, double timeout, Transport transport) {
        if (!Double.isFinite(timeout) || timeout < 1 || timeout > 120 || transport == null) {
            throw new FxMacroDataException("Timeout must be between 1 and 120 seconds and transport must be available.");
        }
        this.apiKey = apiKey == null ? "" : apiKey;
        this.timeout = timeout;
        this.transport = transport;
    }

    public static FxMacroDataClient fromEnvironment(String variable, double timeout) {
        return new FxMacroDataClient(environmentKey(variable), timeout);
    }

    public static String environmentKey(String variable) {
        if (variable == null || variable.isEmpty()) {
            return "";
        }
        if (!variable.matches("[A-Za-z_][A-Za-z0-9_]{0,127}")) {
            throw new FxMacroDataException("Configure an environment variable name, not a credential.");
        }
        String value = System.getenv(variable);
        if (value == null && variable.equals("FXMACRODATA_API_KEY")) {
            value = System.getenv("FXMD_API_KEY");
        }
        if (value == null && !variable.equals("FXMACRODATA_API_KEY")) {
            throw new FxMacroDataException("The configured credential environment variable is unavailable.");
        }
        return value == null ? "" : value;
    }

    public static List<Operation> operations() {
        return List.copyOf(OPERATIONS.values());
    }

    public static Operation operation(String name) {
        Operation value = OPERATIONS.get(name);
        if (value == null) {
            throw new FxMacroDataException("Unknown FXMacroData operation. Use the operation catalogue.");
        }
        return value;
    }

    public static Map<String, Object> arguments(String text) {
        Object value = parse(text == null || text.isBlank() ? "{}" : text);
        if (!(value instanceof Map<?, ?> map)) {
            throw new FxMacroDataException("Operation parameters must be a JSON object.");
        }
        Map<String, Object> result = new LinkedHashMap<>();
        map.forEach((key, item) -> result.put(String.valueOf(key), item));
        return result;
    }

    public static void validate(String name, Map<String, Object> arguments) {
        Operation operation = operation(name);
        if (arguments == null) {
            throw new FxMacroDataException("Operation parameters must be a JSON object.");
        }
        for (String key : arguments.keySet()) {
            if (key == null || Set.of("api_key", "access_token", "authorization", "url", "base_url").contains(key.toLowerCase())) {
                throw new FxMacroDataException("Credentials and endpoint overrides are not operation parameters.");
            }
        }
        try {
            if (!SCHEMAS.getSchema(JSON.valueToTree(operation.inputSchema())).validate(JSON.valueToTree(arguments)).isEmpty()) {
                throw new IllegalArgumentException();
            }
        } catch (Exception error) {
            throw new FxMacroDataException("Invalid FXMacroData parameters. Check the operation input schema.");
        }
    }

    public synchronized Result execute(String name, Map<String, Object> arguments) {
        if (closed) {
            throw new FxMacroDataException("The FXMacroData client is closed.");
        }
        validate(name, arguments);
        Operation operation = operation(name);
        if (operation.method().equals("MCP")) {
            Object result = rpc("tools/call", Map.of("name", name.substring(4), "arguments", arguments), initializeMcp(), false).value;
            if (!(result instanceof Map<?, ?> map) || Boolean.TRUE.equals(map.get("isError"))) {
                throw new FxMacroDataException("FXMacroData MCP could not complete this operation. Check parameters and subscription access.");
            }
            return new Result(name, safe(result), WEBSITE);
        }
        String path = operation.path();
        Map<String, Object> query = new LinkedHashMap<>();
        Map<String, String> headers = new LinkedHashMap<>(Map.of("Accept", "application/json"));
        Object body = null;
        for (Map<String, Object> parameter : operation.parameters()) {
            String key = String.valueOf(parameter.get("name"));
            Object value = arguments.get(key);
            if (value == null) {
                continue;
            }
            switch (String.valueOf(parameter.get("in"))) {
                case "path" -> {
                    if (!(value instanceof String text) || !text.matches("[A-Za-z0-9_.:-]+") || text.equals(".") || text.equals("..")) {
                        throw new FxMacroDataException("Invalid FXMacroData path parameter.");
                    }
                    path = path.replace("{" + key + "}", encode(String.valueOf(value)));
                }
                case "header" -> {
                    if (String.valueOf(value).contains("\r") || String.valueOf(value).contains("\n")) {
                        throw new FxMacroDataException("Invalid event cursor.");
                    }
                    headers.put(key, String.valueOf(value));
                }
                case "body" -> body = value;
                default -> query.put(key, value);
            }
        }
        if (name.equals("stream_events")) {
            double seconds = Math.min(timeout, number(arguments.getOrDefault("max_seconds", 10)).doubleValue());
            int count = number(arguments.getOrDefault("max_events", 10)).intValue();
            headers.put("Accept", "text/event-stream");
            try (Response response = request(operation.method(), API_ORIGIN + path, query, headers, body, seconds)) {
                checkStatus(response.status);
                List<Object> events = new ArrayList<>();
                AtomicBoolean expired = new AtomicBoolean();
                readSse(response.body, seconds, count, event -> { events.add(event); return events.size() < count; }, expired);
                return new Result(name, safe(Map.of("events", events, "capture_complete", events.size() >= count,
                        "reason", events.size() >= count ? "event_limit" : expired.get() ? "timeout" : "stream_end_or_deadline")), API_ORIGIN + path);
            } catch (FxMacroDataException error) {
                throw error;
            } catch (Exception error) {
                throw new FxMacroDataException("FXMacroData returned an unavailable event stream.");
            }
        }
        try (Response response = request(operation.method(), API_ORIGIN + path, query, headers, body, timeout)) {
            return new Result(name, safe(readJson(response, null)), API_ORIGIN + path);
        }
    }

    /** Collect every offset page within a caller-selected budget, preserving page metadata. */
    public synchronized Result history(String name, Map<String, Object> arguments, int maxPages) {
        validate(name, arguments);
        Operation operation = operation(name);
        if (!operation.method().equals("GET") || operation.parameters().stream().noneMatch(p -> "offset".equals(p.get("name")))) {
            throw new FxMacroDataException("Full history requires an offset-paginated REST operation.");
        }
        if (maxPages < 1 || maxPages > 10000 || arguments.containsKey("page")
                || new BigDecimal(number(arguments.getOrDefault("offset", 0)).toString()).compareTo(BigDecimal.ZERO) != 0) {
            throw new FxMacroDataException("Full history must start at offset zero, without a page, within a 1–10000 page budget.");
        }
        Map<String, Object> query = new LinkedHashMap<>(arguments);
        query.putIfAbsent("limit", 100);
        List<Object> rows = new ArrayList<>();
        List<Object> pages = new ArrayList<>();
        long offset = 0;
        String version = null;
        Long advertisedTotal = null;
        Set<String> seenPages = new LinkedHashSet<>();
        String source = WEBSITE;
        for (int page = 0; page < maxPages; page++) {
            query.put("offset", offset);
            Result result = execute(name, query);
            source = result.sourceUrl();
            if (!(result.payload() instanceof Map<?, ?> payload) || !(payload.get("pagination") instanceof Map<?, ?> pagination)) {
                throw new FxMacroDataException("FXMacroData history response did not include pagination metadata.");
            }
            if (!(payload.get("data") instanceof List<?> batch) || !(pagination.get("has_more") instanceof Boolean more)) {
                throw new FxMacroDataException("FXMacroData history response omitted its data array or continuation metadata.");
            }
            if ("source".equals(query.get("value_mode")) && (!"source".equals(payload.get("value_mode"))
                    || !(payload.get("value_metadata") instanceof Map<?, ?> valueMetadata)
                    || !Boolean.FALSE.equals(valueMetadata.get("normalization_applied")))) {
                throw new FxMacroDataException("FXMacroData history did not confirm source-unit data.");
            }
            Object responseVersion = payload.get("replay") instanceof Map<?, ?> replay ? replay.get("dataset_version") : null;
            if (responseVersion == null) responseVersion = payload.get("dataset_version");
            if (version != null && !version.equals(responseVersion)) {
                throw new FxMacroDataException("FXMacroData history content version changed or disappeared. Restart the download.");
            }
            if (responseVersion instanceof String current) {
                if (version != null && !version.equals(current)) {
                    throw new FxMacroDataException("FXMacroData history changed during collection. Restart the download.");
                }
                version = current;
                query.put("dataset_version", version);
            }
            rows.addAll(batch);
            Map<String, Object> metadata = new LinkedHashMap<>();
            payload.forEach((key, value) -> { if (!"data".equals(String.valueOf(key))) metadata.put(String.valueOf(key), value); });
            pages.add(metadata);
            Long total = pagination.get("total_count") == null ? null : integer(pagination.get("total_count"));
            long returned = pagination.get("returned_count") == null ? batch.size() : integer(pagination.get("returned_count"));
            long actualOffset = pagination.get("offset") == null ? offset : integer(pagination.get("offset"));
            if (advertisedTotal != null && !advertisedTotal.equals(total)) {
                throw new FxMacroDataException("FXMacroData history row count changed. Restart the download.");
            }
            advertisedTotal = total;
            if (actualOffset != offset || returned != batch.size() || returned < 0 || total != null && total < offset + returned) {
                throw new FxMacroDataException("FXMacroData returned inconsistent pagination metadata.");
            }
            if (!batch.isEmpty() && !seenPages.add(json(batch))) throw new FxMacroDataException("FXMacroData repeated a history page.");
            offset += returned;
            if (!more) {
                if (total != null && offset != total) throw new FxMacroDataException("FXMacroData history stopped before its advertised total.");
                return new Result(name, Map.of("data", rows, "pages", pages, "complete", true, "rows_returned", rows.size(), "pages_fetched", page + 1), source);
            }
            if (returned == 0 || pagination.get("next_offset") == null || integer(pagination.get("next_offset")) != offset) {
                throw new FxMacroDataException("FXMacroData pagination did not advance contiguously.");
            }
        }
        throw new FxMacroDataException("Full history exceeded the explicit page budget. Increase maxPages or request individual pages.");
    }

    private Map<String, String> initializeMcp() {
        if (mcpHeaders == null) {
            Map<String, String> headers = new LinkedHashMap<>(Map.of("Accept", "application/json, text/event-stream"));
            Rpc result = rpc("initialize", Map.of("protocolVersion", PROTOCOL, "capabilities", Map.of(), "clientInfo", Map.of("name", "fxmacrodata-java-client", "version", "0.1.0")), headers, false);
            if (!(result.value instanceof Map<?, ?> map) || !PROTOCOL.equals(map.get("protocolVersion"))) {
                throw new FxMacroDataException("FXMacroData returned an unsupported MCP protocol version.");
            }
            headers.put("MCP-Protocol-Version", PROTOCOL);
            String session = result.headers.entrySet().stream().filter(e -> e.getKey().equalsIgnoreCase("Mcp-Session-Id")).map(Map.Entry::getValue).findFirst().orElse(null);
            if (session != null) {
                if (!session.matches("[\\x21-\\x7e]+")) {
                    throw new FxMacroDataException("FXMacroData returned an invalid MCP session identifier.");
                }
                headers.put("Mcp-Session-Id", session);
            }
            rpc("notifications/initialized", Map.of(), headers, true);
            mcpHeaders = headers;
        }
        return mcpHeaders;
    }

    private record Rpc(Object value, Map<String, String> headers) { }

    private Rpc rpc(String method, Map<String, Object> parameters, Map<String, String> headers, boolean notification) {
        Map<String, Object> body = new LinkedHashMap<>(Map.of("jsonrpc", "2.0", "method", method, "params", parameters));
        Long id = notification ? null : ++requestId;
        if (id != null) {
            body.put("id", id);
        }
        try (Response response = request("POST", MCP_URL, Map.of(), headers, body, timeout)) {
            checkStatus(response.status);
            if (notification) {
                return new Rpc(null, response.headers);
            }
            Object value = readJson(response, id);
            if (!(value instanceof Map<?, ?> map) || !"2.0".equals(map.get("jsonrpc")) || !map.containsKey("result")
                    || map.containsKey("error") || !(map.get("id") instanceof Number number) || integer(number) != id) {
                throw new FxMacroDataException("FXMacroData MCP could not complete this request.");
            }
            return new Rpc(map.get("result"), response.headers);
        }
    }

    private Object readJson(Response response, Long id) {
        checkStatus(response.status);
        String contentType = response.headers.entrySet().stream().filter(e -> e.getKey().equalsIgnoreCase("Content-Type")).map(Map.Entry::getValue).findFirst().orElse("");
        if (contentType.toLowerCase().contains("text/event-stream")) {
            List<Object> values = new ArrayList<>();
            readSse(response.body, timeout, Integer.MAX_VALUE, event -> {
                Object value = event.get("data");
                if (value instanceof Map<?, ?> map && (id == null || map.get("id") instanceof Number n && integer(n) == id)) {
                    values.add(value);
                    return false;
                }
                return true;
            }, new AtomicBoolean());
            if (values.isEmpty()) {
                throw new FxMacroDataException("FXMacroData returned no matching MCP response.");
            }
            return values.getFirst();
        }
        AtomicBoolean expired = new AtomicBoolean();
        var deadline = timer.schedule(() -> { expired.set(true); closeQuietly(response.body); }, (long) (timeout * 1000), TimeUnit.MILLISECONDS);
        try {
            byte[] bytes = response.body.readNBytes(MAX_RESPONSE_BYTES + 1);
            if (bytes.length > MAX_RESPONSE_BYTES || expired.get()) {
                throw new FxMacroDataException("FXMacroData response exceeded its size or time budget.");
            }
            return parse(utf8(bytes));
        } catch (IOException error) {
            throw new FxMacroDataException("FXMacroData returned an unavailable response.");
        } finally {
            deadline.cancel(false);
        }
    }

    @FunctionalInterface
    private interface EventHandler { boolean accept(Map<String, Object> event); }

    private void readSse(InputStream input, double seconds, int count, EventHandler handler, AtomicBoolean expired) {
        var deadline = timer.schedule(() -> { expired.set(true); closeQuietly(input); }, Math.max(1, (long) (seconds * 1000)), TimeUnit.MILLISECONDS);
        ByteArrayOutputStream line = new ByteArrayOutputStream();
        List<String> data = new ArrayList<>();
        Map<String, Object> event = new LinkedHashMap<>();
        int bytes = 0;
        int events = 0;
        boolean skipLf = false;
        try {
            while (!expired.get()) {
                int item = input.read();
                if (item == -1) {
                    break;
                }
                if (++bytes > MAX_RESPONSE_BYTES) {
                    throw new FxMacroDataException("FXMacroData event stream exceeded its response size budget.");
                }
                if (skipLf && item == '\n') {
                    skipLf = false;
                    continue;
                }
                skipLf = item == '\r';
                if (item != '\n' && item != '\r') {
                    line.write(item);
                    continue;
                }
                String text = utf8(line.toByteArray());
                line.reset();
                if (text.isEmpty()) {
                    if (!data.isEmpty()) {
                        String joined = String.join("\n", data);
                        try { event.put("data", parse(joined)); } catch (FxMacroDataException ignored) { event.put("data", joined); }
                        if (!handler.accept(event) || ++events >= count) {
                            break;
                        }
                    }
                    data = new ArrayList<>();
                    event = new LinkedHashMap<>();
                } else if (!text.startsWith(":")) {
                    int colon = text.indexOf(':');
                    String key = colon < 0 ? text : text.substring(0, colon);
                    String value = colon < 0 ? "" : text.substring(colon + 1);
                    if (value.startsWith(" ")) value = value.substring(1);
                    if (key.equals("data")) data.add(value);
                    else if (Set.of("id", "event", "retry").contains(key)) event.put(key, value);
                }
            }
        } catch (IOException error) {
            if (!expired.get()) throw new FxMacroDataException("FXMacroData returned an invalid event stream.");
        } finally {
            deadline.cancel(false);
        }
    }

    private Response request(String method, String originPath, Map<String, Object> query, Map<String, String> headers, Object body, double seconds) {
        if (!(originPath.startsWith(API_ORIGIN + "/v1/") || originPath.equals(MCP_URL))) {
            throw new FxMacroDataException("Unsupported FXMacroData endpoint.");
        }
        Map<String, Object> parameters = new LinkedHashMap<>(query);
        if (!apiKey.isEmpty()) parameters.put("api_key", apiKey);
        StringBuilder url = new StringBuilder(originPath);
        for (var entry : parameters.entrySet()) {
            if (entry.getValue() == null) continue;
            url.append(url.indexOf("?") < 0 ? '?' : '&').append(encode(entry.getKey())).append('=').append(encode(String.valueOf(entry.getValue())));
        }
        Map<String, String> requestHeaders = new LinkedHashMap<>(headers);
        requestHeaders.put("Content-Type", "application/json");
        try {
            return transport.send(new Request(method, URI.create(url.toString()), requestHeaders, body == null ? null : json(body), seconds));
        } catch (Exception error) {
            throw new FxMacroDataException("Unable to reach FXMacroData within the request timeout. Retry later.");
        }
    }

    private static void checkStatus(int status) {
        if (status >= 200 && status < 300) return;
        if (status == 401 || status == 403) throw new FxMacroDataException("This dataset requires an authorized FXMacroData subscription API key.");
        if (status == 429) throw new FxMacroDataException("FXMacroData rate limit reached. Retry later.");
        if (status >= 300 && status < 400) throw new FxMacroDataException("FXMacroData redirects are not followed.");
        throw new FxMacroDataException("FXMacroData request failed (HTTP " + status + ").");
    }

    private Object safe(Object value) {
        if (value instanceof String text) return redact(text);
        if (value instanceof List<?> list) return list.stream().map(this::safe).toList();
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> result = new LinkedHashMap<>();
            map.forEach((key, item) -> result.put(redact(String.valueOf(key)), SECRET_FIELDS.contains(String.valueOf(key).toLowerCase().replace("_", "").replace("-", "")) ? "[redacted]" : safe(item)));
            return result;
        }
        return value;
    }

    private String redact(String text) {
        if (!apiKey.isEmpty()) {
            text = text.replace(apiKey, "[redacted]");
            String encoded = encode(apiKey);
            StringBuilder pattern = new StringBuilder();
            for (int i = 0; i < encoded.length();) {
                if (encoded.charAt(i) == '%' && i + 2 < encoded.length()) {
                    pattern.append("(?i:").append(Pattern.quote(encoded.substring(i, i + 3))).append(")");
                    i += 3;
                } else {
                    pattern.append(Pattern.quote(encoded.substring(i, i + 1)));
                    i++;
                }
            }
            text = text.replaceAll(pattern.toString(), "[redacted]");
            if (encoded.contains("%20")) text = text.replace(URLEncoder.encode(apiKey, StandardCharsets.UTF_8), "[redacted]");
        }
        return text.replaceAll("(?i)(api_key|access_token|authorization)=([^\\s&\\\"']+)", "$1=[redacted]");
    }

    public static String json(Object value) {
        try { return JSON.writeValueAsString(value); }
        catch (Exception error) { throw new FxMacroDataException("Cannot serialize the FXMacroData data object."); }
    }

    public static Object parse(String value) {
        if (value == null || value.length() > MAX_RESPONSE_BYTES) throw new FxMacroDataException("Invalid or oversized JSON data.");
        try { return JSON.readValue(value, Object.class); }
        catch (Exception error) { throw new FxMacroDataException("Invalid FXMacroData JSON data."); }
    }

    public static Map<String, Object> immutableMap(Map<String, Object> value) {
        Map<String, Object> result = new LinkedHashMap<>();
        value.forEach((key, item) -> result.put(key, immutable(item)));
        return Collections.unmodifiableMap(result);
    }

    public static Object immutable(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> result = new LinkedHashMap<>();
            map.forEach((key, item) -> result.put(String.valueOf(key), immutable(item)));
            return Collections.unmodifiableMap(result);
        }
        if (value instanceof List<?> list) return list.stream().map(FxMacroDataClient::immutable).toList();
        return value;
    }

    private static Number number(Object value) {
        if (!(value instanceof Number number)) throw new FxMacroDataException("FXMacroData pagination requires numeric metadata and limits.");
        return number;
    }

    private static long integer(Object value) {
        try { return new BigDecimal(number(value).toString()).longValueExact(); }
        catch (ArithmeticException error) { throw new FxMacroDataException("FXMacroData pagination and identifiers require exact integers."); }
    }

    private static String utf8(byte[] bytes) {
        try { return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString(); }
        catch (Exception error) { throw new FxMacroDataException("FXMacroData returned invalid UTF-8 data."); }
    }

    private static Map<String, Operation> loadOperations() {
        Map<String, Operation> result = new LinkedHashMap<>();
        for (String file : List.of("operations.json", "mcp-tools.json")) {
            try (InputStream input = FxMacroDataClient.class.getResourceAsStream("/com/fxmacrodata/client/" + file)) {
                List<Map<String, Object>> records = JSON.readValue(input, new TypeReference<>() { });
                for (Map<String, Object> item : records) {
                    boolean mcp = file.equals("mcp-tools.json");
                    String name = (mcp ? "mcp_" : "") + item.get("name");
                    @SuppressWarnings("unchecked") Map<String, Object> schema = (Map<String, Object>) item.get(mcp ? "inputSchema" : "input_schema");
                    @SuppressWarnings("unchecked") List<Map<String, Object>> parameters = (List<Map<String, Object>>) item.getOrDefault("parameters", List.of());
                    result.put(name, new Operation(name, String.valueOf(item.get("description")), mcp ? "MCP" : String.valueOf(item.get("method")), mcp ? "/mcp" : String.valueOf(item.get("path")), schema, parameters));
                }
            } catch (Exception error) {
                throw new ExceptionInInitializerError("FXMacroData operation catalogue is unavailable.");
            }
        }
        return Collections.unmodifiableMap(result);
    }

    private static String encode(String value) { return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20"); }
    private static void closeQuietly(InputStream input) { try { input.close(); } catch (IOException ignored) { } }

    @Override public String toString() { return "FxMacroDataClient(authenticated=" + !apiKey.isEmpty() + ")"; }

    @Override public synchronized void close() {
        if (closed) return;
        if (mcpHeaders != null && mcpHeaders.containsKey("Mcp-Session-Id")) {
            try (Response ignored = request("DELETE", MCP_URL, Map.of(), mcpHeaders, null, Math.min(timeout, 2))) { }
            catch (Exception ignored) { }
        }
        mcpHeaders = null;
        closed = true;
        timer.shutdownNow();
        try { transport.close(); } catch (Exception ignored) { }
    }

    public record Request(String method, URI uri, Map<String, String> headers, String body, double timeout) {
        public Request { headers = Map.copyOf(headers); }
        @Override public String toString() { return "FXMacroData request (" + method + ")"; }
    }

    public static final class Response implements AutoCloseable {
        public final int status;
        public final Map<String, String> headers;
        public final InputStream body;
        public Response(int status, Map<String, String> headers, InputStream body) { this.status = status; this.headers = Map.copyOf(headers); this.body = body; }
        public static Response json(int status, Object value) { return new Response(status, Map.of("Content-Type", "application/json"), new ByteArrayInputStream(FxMacroDataClient.json(value).getBytes(StandardCharsets.UTF_8))); }
        @Override public void close() { closeQuietly(body); }
        @Override public String toString() { return "FXMacroData response (HTTP " + status + ")"; }
    }

    public interface Transport extends AutoCloseable {
        Response send(Request request) throws Exception;
        @Override default void close() { }
    }

    private static final class JdkTransport implements Transport {
        private final HttpClient client;
        JdkTransport(double seconds) {
            if (!Double.isFinite(seconds) || seconds < 1 || seconds > 120) throw new FxMacroDataException("Timeout must be between 1 and 120 seconds.");
            client = HttpClient.newBuilder().connectTimeout(Duration.ofMillis((long) (seconds * 1000))).followRedirects(HttpClient.Redirect.NEVER).build();
        }
        @Override public Response send(Request value) throws Exception {
            var builder = HttpRequest.newBuilder(value.uri).timeout(Duration.ofMillis((long) (value.timeout * 1000)));
            value.headers.forEach(builder::header);
            builder.method(value.method, value.body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(value.body));
            HttpResponse<InputStream> response = client.send(builder.build(), HttpResponse.BodyHandlers.ofInputStream());
            Map<String, String> headers = new LinkedHashMap<>();
            response.headers().map().forEach((key, values) -> headers.put(key, values.getFirst()));
            return new Response(response.statusCode(), headers, response.body());
        }
        @Override public void close() { client.close(); }
    }
}
