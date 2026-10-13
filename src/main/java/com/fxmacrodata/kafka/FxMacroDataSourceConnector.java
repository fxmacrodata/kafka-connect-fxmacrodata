package com.fxmacrodata.kafka;

import com.fxmacrodata.client.FxMacroDataClient;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.ExactlyOnceSupport;
import org.apache.kafka.connect.source.SourceConnector;

/** Kafka Connect source for public FXMacroData operations. */
public final class FxMacroDataSourceConnector extends SourceConnector {
    private Map<String, String> properties = Map.of();
    private List<StreamSpec> streams = List.of();

    @Override public String version() { return "0.1.0"; }
    @Override public Class<? extends Task> taskClass() { return FxMacroDataSourceTask.class; }
    @Override public ConfigDef config() { return ConnectorConfig.definition(); }
    @Override public ExactlyOnceSupport exactlyOnceSupport(Map<String, String> ignored) { return ExactlyOnceSupport.UNSUPPORTED; }

    @Override public void start(Map<String, String> props) {
        ConnectorConfig checked = ConnectorConfig.parse(props);
        properties = checked.safeProperties();
        streams = checked.streams();
    }

    @Override public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (maxTasks < 1 || streams.isEmpty()) return List.of();
        int count = Math.min(maxTasks, streams.size());
        List<List<StreamSpec>> groups = new ArrayList<>();
        for (int i = 0; i < count; i++) groups.add(new ArrayList<>());
        for (int i = 0; i < streams.size(); i++) groups.get(i % count).add(streams.get(i));
        return groups.stream().map(group -> {
            Map<String, String> result = new LinkedHashMap<>(properties);
            result.put("streams", FxMacroDataClient.json(group.stream().map(StreamSpec::asMap).toList()));
            return result;
        }).toList();
    }

    @Override public void stop() { properties = Map.of(); streams = List.of(); }
}
