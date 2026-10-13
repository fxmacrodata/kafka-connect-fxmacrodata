package com.fxmacrodata.client;

import java.util.List;
import java.util.Map;

/** Original public operation metadata and schema. */
public record Operation(String name, String description, String method, String path,
                        Map<String, Object> inputSchema, List<Map<String, Object>> parameters) {
    public Operation {
        inputSchema = FxMacroDataClient.immutableMap(inputSchema);
        parameters = parameters.stream().map(FxMacroDataClient::immutableMap).toList();
    }
}
