package com.fxmacrodata.kafka;

import com.fxmacrodata.client.FxMacroDataClient;
import java.util.Map;

/** Print public operation schemas for connector configuration tooling. */
public final class OperationCatalogue {
    private OperationCatalogue() {}
    public static void main(String[] args) {
        System.out.println(FxMacroDataClient.json(FxMacroDataClient.operations().stream().map(operation -> Map.of(
            "name", operation.name(), "description", operation.description(), "method", operation.method(),
            "path", operation.path(), "input_schema", operation.inputSchema())).toList()));
    }
}
