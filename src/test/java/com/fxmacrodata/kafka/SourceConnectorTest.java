package com.fxmacrodata.kafka;

import com.fxmacrodata.client.FxMacroDataClient;
import com.fxmacrodata.client.Result;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.ExactlyOnceSupport;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

final class SourceConnectorTest {
    private static final String STREAM = "[{\"operation\":\"data_catalogue\",\"arguments\":{\"currency\":\"usd\"},\"topic\":\"macro.usd\"}]";
    private static Map<String, String> props(int batch) { return Map.of("streams", STREAM, "batch.size", String.valueOf(batch), "poll.interval.ms", "1000"); }
    private static Result result() { return new Result("data_catalogue", Map.of("data", List.of(
        Map.of("date", "2026-01-01", "value", new BigDecimal("1.0000000000000000001"), "extra", Map.of("status", "synthetic")),
        Map.of("date", "2026-01-02", "value", 2)), "metadata", Map.of("publication_time_status", "unknown")), "https://api.fxmacrodata.com/v1/data_catalogue/usd"); }
    private static SnapshotReader fixed(Result result) { return new SnapshotReader() { public Result read(StreamSpec stream, int pages) { return result; } public void close() {} }; }
    private static SourceTaskContext context(Map<String, ?> offset) {
        OffsetStorageReader storage = (OffsetStorageReader) Proxy.newProxyInstance(OffsetStorageReader.class.getClassLoader(), new Class<?>[]{OffsetStorageReader.class},
            (proxy, method, args) -> method.getName().equals("offset") ? offset : Map.of());
        return (SourceTaskContext) Proxy.newProxyInstance(SourceTaskContext.class.getClassLoader(), new Class<?>[]{SourceTaskContext.class},
            (proxy, method, args) -> method.getName().equals("offsetStorageReader") ? storage : method.getName().equals("configs") ? Map.of() : null);
    }
    private static FxMacroDataSourceTask task(Result result, Map<String, ?> committed, int batch) {
        FxMacroDataSourceTask task = new FxMacroDataSourceTask(fixed(result));
        task.initialize(context(committed)); task.start(props(batch)); return task;
    }

    @Test void currentPublicContractIsDiscoverable() {
        assertEquals(87, FxMacroDataClient.operations().size());
        assertEquals(33, FxMacroDataClient.operations().stream().filter(op -> !op.method().equals("MCP")).count());
        assertEquals(54, FxMacroDataClient.operations().stream().filter(op -> op.method().equals("MCP")).count());
        for (String name : List.of("calendar_snapshot", "calendar_event_versions", "calendar_archive_coverage", "risk_sentiment_changes"))
            assertNotNull(FxMacroDataClient.operation("mcp_" + name));
    }

    @Test void nativeRegistrationAndTaskDistribution() {
        FxMacroDataSourceConnector connector = new FxMacroDataSourceConnector();
        String streams = FxMacroDataClient.json(List.of(Map.of("operation", "data_catalogue", "arguments", Map.of("currency", "usd"), "topic", "one"),
            Map.of("operation", "release_calendar", "arguments", Map.of("currency", "usd"), "topic", "two")));
        connector.start(Map.of("streams", streams, "api.key.env", "FXMD_API_KEY"));
        assertEquals(FxMacroDataSourceTask.class, connector.taskClass());
        assertEquals(2, connector.taskConfigs(8).size());
        assertEquals(1, connector.taskConfigs(1).size());
        assertTrue(connector.taskConfigs(2).stream().allMatch(p -> p.get("api.key.env").equals("FXMD_API_KEY")));
        assertEquals(ExactlyOnceSupport.UNSUPPORTED, connector.exactlyOnceSupport(Map.of()));
        assertNotNull(getClass().getResource("/META-INF/services/org.apache.kafka.connect.connector.Connector"));
    }

    @Test void envelopePreservesRecordMetadataAndExactValue() throws Exception {
        FxMacroDataSourceTask task = task(result(), null, 1);
        SourceRecord record = task.poll().getFirst();
        Struct value = (Struct) record.value();
        assertEquals("1.0000000000000000001", value.getString("value_text"));
        assertTrue(value.getString("record_json").contains("synthetic"));
        assertTrue(value.getString("response_json").contains("publication_time_status"));
        assertEquals("usd", value.getString("currency"));
        assertEquals(0L, record.sourceOffset().get("index"));
        assertEquals(false, record.sourceOffset().get("snapshot_complete"));
        assertEquals(record.key(), value.getString("record_id"));
        assertEquals(1L, task.poll().getFirst().sourceOffset().get("index"));
        task.stop();
    }

    @Test void restartUsesOnlyCommittedOffset() throws Exception {
        FxMacroDataSourceTask first = task(result(), null, 1);
        SourceRecord committed = first.poll().getFirst();
        first.poll(); // A later uncommitted record is deliberately not supplied on restart.
        first.stop();
        FxMacroDataSourceTask restarted = task(result(), committed.sourceOffset(), 10);
        List<SourceRecord> records = restarted.poll();
        assertEquals(1, records.size());
        assertEquals("2026-01-02", ((Struct) records.getFirst().value()).getString("date"));
        restarted.stop();
    }

    @Test void changedSnapshotReplaysFromBeginning() throws Exception {
        FxMacroDataSourceTask first = task(result(), null, 1);
        SourceRecord committed = first.poll().getFirst(); first.stop();
        Result changed = new Result("data_catalogue", Map.of("data", List.of(Map.of("date", "2026-01-01", "value", 3))), "https://api.fxmacrodata.com/v1/data_catalogue/usd");
        FxMacroDataSourceTask restarted = task(changed, committed.sourceOffset(), 10);
        SourceRecord row = restarted.poll().getFirst();
        assertEquals(0L, row.sourceOffset().get("index"));
        assertEquals("3", ((Struct) row.value()).getString("value_text")); restarted.stop();
    }

    @Test void emptyDataStillEmitsMetadataEnvelope() throws Exception {
        Result empty = new Result("data_catalogue", Map.of("data", List.of(), "coverage", Map.of("available", false)), "https://api.fxmacrodata.com/v1/data_catalogue/usd");
        FxMacroDataSourceTask task = task(empty, null, 10);
        Struct value = (Struct) task.poll().getFirst().value();
        assertNull(value.get("record_json")); assertTrue(value.getString("response_json").contains("coverage")); task.stop();
    }

    @Test void snapshotBudgetFailsBeforeEmission() {
        FxMacroDataSourceTask task = new FxMacroDataSourceTask(fixed(result()));
        task.initialize(context(null)); task.start(Map.of("streams", STREAM, "max.records", "1"));
        assertThrows(ConnectException.class, task::poll); task.stop();
    }

    @Test void unsafeConfigAndNestedCredentialFieldsAreRejectedWithoutEcho() {
        ConfigException error = assertThrows(ConfigException.class, () -> ConnectorConfig.parse(Map.of("api.key.env", "not a variable name")));
        assertFalse(error.getMessage().contains("not a variable name")); assertNull(error.getCause());
        assertThrows(ConfigException.class, () -> ConnectorConfig.parse(Map.of("streams", "[{\"operation\":\"data_catalogue\",\"topic\":\"t\",\"arguments\":{\"currency\":\"usd\",\"api_key\":\"synthetic-fixture\"}}]")));
        assertThrows(ConfigException.class, () -> ConnectorConfig.parse(Map.of("batch.size", "0")));
        assertThrows(ConfigException.class, () -> ConnectorConfig.parse(Map.of("streams", STREAM.substring(0, STREAM.length()-1) + "," + STREAM.substring(1))));
    }

    @Test void upstreamErrorsDoNotLeakMessagesOrCauses() {
        SnapshotReader reader = new SnapshotReader() { public Result read(StreamSpec stream, int pages) { throw new IllegalStateException("sensitive-fixture-context"); } public void close() {} };
        FxMacroDataSourceTask task = new FxMacroDataSourceTask(reader); task.initialize(context(null)); task.start(props(1));
        RetriableException error = assertThrows(RetriableException.class, task::poll);
        assertFalse(error.getMessage().contains("sensitive-fixture-context")); assertNull(error.getCause()); task.stop();
    }

    @Test void sourcePartitionsAreStableAcrossObjectKeyOrder() {
        StreamSpec one = StreamSpec.parse(Map.of("operation", "data_catalogue", "topic", "t", "arguments", Map.of("currency", "usd")));
        StreamSpec two = StreamSpec.parse(Map.of("arguments", Map.of("currency", "usd"), "topic", "t", "operation", "data_catalogue"));
        assertEquals(one.partition(), two.partition());
    }
}
