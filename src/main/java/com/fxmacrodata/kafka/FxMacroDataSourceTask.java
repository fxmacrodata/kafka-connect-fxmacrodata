package com.fxmacrodata.kafka;

import com.fxmacrodata.client.FxMacroDataClient;
import com.fxmacrodata.client.Operation;
import com.fxmacrodata.client.Result;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

/** Bounded snapshots with Kafka-managed acknowledgement-safe offsets. */
public final class FxMacroDataSourceTask extends SourceTask {
    public static final Schema VALUE_SCHEMA = SchemaBuilder.struct().name("com.fxmacrodata.Record").version(1)
        .field("operation", Schema.STRING_SCHEMA).field("source_url", Schema.STRING_SCHEMA)
        .field("record_id", Schema.STRING_SCHEMA).field("snapshot_id", Schema.STRING_SCHEMA)
        .field("currency", Schema.OPTIONAL_STRING_SCHEMA).field("indicator", Schema.OPTIONAL_STRING_SCHEMA)
        .field("date", Schema.OPTIONAL_STRING_SCHEMA).field("announcement_datetime", Schema.OPTIONAL_STRING_SCHEMA)
        .field("value", Schema.OPTIONAL_FLOAT64_SCHEMA).field("value_text", Schema.OPTIONAL_STRING_SCHEMA).field("record_json", Schema.OPTIONAL_STRING_SCHEMA)
        .field("response_json", Schema.STRING_SCHEMA).build();
    private ConnectorConfig config;
    private SnapshotReader reader;
    private final SnapshotReader suppliedReader;
    private final List<State> states = new ArrayList<>();
    private volatile boolean stopped;
    private int cursor;
    private final Object wakeup = new Object();

    public FxMacroDataSourceTask() { suppliedReader = null; }
    FxMacroDataSourceTask(SnapshotReader reader) { suppliedReader = reader; }
    @Override public String version() { return "0.1.0"; }

    @Override public void start(Map<String, String> props) {
        config = ConnectorConfig.parse(props);
        if (suppliedReader != null) reader = suppliedReader;
        else {
            FxMacroDataClient client = FxMacroDataClient.fromEnvironment(config.apiKeyEnv(), config.timeout());
            reader = new SnapshotReader() {
                public Result read(StreamSpec stream, int maxPages) {
                    return SnapshotCollector.collect(client, stream, maxPages, config.maxRecords(), config.maxSnapshotBytes());
                }
                public void close() { client.close(); }
            };
        }
        states.clear(); stopped = false; cursor = 0;
        for (StreamSpec stream : config.streams()) {
            Map<String, Object> committed = context == null ? null : context.offsetStorageReader().offset(stream.partition());
            states.add(new State(stream, committed));
        }
    }

    @Override public List<SourceRecord> poll() throws InterruptedException {
        if (stopped) return null;
        List<SourceRecord> output = new ArrayList<>();
        long now = System.currentTimeMillis();
        for (int visited = 0; visited < states.size() && output.size() < config.batchSize(); visited++) {
            State state = states.get(Math.floorMod(cursor++, states.size()));
            if (state.rows == null && state.nextPoll <= now) load(state, now);
            while (state.rows != null && state.index < state.rows.size() && output.size() < config.batchSize()) {
                Map<String, Object> row = state.rows.get(state.index);
                String record = row == null ? null : FxMacroDataClient.json(row);
                String recordId = StreamSpec.digest(state.stream.id() + "\n" + (record == null ? "null" : FxMacroDataClient.json(StreamSpec.canonical(row))));
                Map<String, Object> offset = Map.of("snapshot_id", state.snapshot, "index", (long) state.index,
                    "snapshot_complete", state.index == state.rows.size() - 1);
                Struct value = new Struct(VALUE_SCHEMA).put("operation", state.stream.operation()).put("source_url", state.source)
                    .put("record_id", recordId).put("snapshot_id", state.snapshot).put("record_json", record).put("response_json", state.response);
                for (String field : List.of("currency", "indicator", "date", "announcement_datetime"))
                    value.put(field, text(row, field, state.stream.arguments().get(field)));
                value.put("value_text", text(row, "value", null));
                Object numeric = row == null ? null : row.get("value");
                value.put("value", numeric instanceof Number number && Double.isFinite(number.doubleValue()) ? number.doubleValue() : null);
                output.add(new SourceRecord(state.stream.partition(), offset, state.stream.topic(), null,
                    Schema.STRING_SCHEMA, recordId, VALUE_SCHEMA, value));
                state.index++;
            }
            if (state.rows != null && state.index >= state.rows.size()) {
                state.previous = state.snapshot; state.rows = null; state.resume = null;
                state.nextPoll = now + config.pollMs();
            }
        }
        if (output.isEmpty()) synchronized (wakeup) { if (!stopped) wakeup.wait(Math.min(config.pollMs(), 1000)); }
        return stopped ? null : output;
    }

    private void load(State state, long now) {
        final Result result;
        try { result = reader.read(state.stream, config.maxPages()); }
        catch (ConnectException safeError) { throw safeError; }
        catch (RuntimeException error) {
            throw new RetriableException("FXMacroData snapshot unavailable. Check operation access and request budgets.");
        }
        String response = FxMacroDataClient.json(result.payload());
        if (response.getBytes(java.nio.charset.StandardCharsets.UTF_8).length > config.maxSnapshotBytes())
            throw new ConnectException("FXMacroData snapshot exceeds max.snapshot.bytes; narrow the requested window or coordinate larger producer and broker message limits.");
        String snapshot = StreamSpec.digest(FxMacroDataClient.json(StreamSpec.canonical(result.payload())));
        if (snapshot.equals(state.previous)) { state.nextPoll = now + config.pollMs(); return; }
        List<Map<String, Object>> records = result.records();
        if (records.size() > config.maxRecords()) throw new ConnectException("FXMacroData snapshot exceeds max.records; narrow arguments or increase the explicit budget.");
        List<Map<String, Object>> rows = records.isEmpty() ? new ArrayList<>() : new ArrayList<>(records);
        if (rows.isEmpty()) rows.add(null); // Persist metadata even when the response has no records.
        int index = 0;
        if (state.resume != null && snapshot.equals(state.resume.get("snapshot_id"))) {
            if (state.resume.get("index") instanceof Number offset && offset.longValue() >= -1 && offset.longValue() < rows.size()) index = (int) offset.longValue() + 1;
            else throw new ConnectException("Invalid FXMacroData stored offset.");
        }
        state.rows = rows; state.index = index; state.response = response; state.snapshot = snapshot; state.source = result.sourceUrl();
    }

    private static String text(Map<String, Object> row, String key, Object fallback) {
        Object value = row != null && row.containsKey(key) ? row.get(key) : fallback;
        return value == null ? null : value.toString();
    }

    @Override public void stop() {
        stopped = true;
        synchronized (wakeup) { wakeup.notifyAll(); }
        if (reader != null) reader.close();
    }

    private static final class State {
        final StreamSpec stream;
        Map<String, Object> resume;
        String previous, snapshot, response, source;
        List<Map<String, Object>> rows;
        int index;
        long nextPoll;
        State(StreamSpec stream, Map<String, Object> resume) { this.stream = stream; this.resume = resume; }
    }
}
