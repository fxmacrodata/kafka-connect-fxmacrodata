The `com.fxmacrodata.client` classes and public operation JSON resources are included from FXMacroData's MIT-licensed Java client, copyright 2026 Robert Tidball. They implement the published FXMacroData client contract. Operation metadata is updated to include the current 33 REST operations and 54 MCP tools.

The connector uses Apache Kafka Connect API 4.1.2 (Apache-2.0, supplied by the worker), Jackson 2.22.2 (Apache-2.0), and networknt JSON Schema Validator 2.0.7 (Apache-2.0). Maven's shade phase retains dependency LICENSE and NOTICE files. JUnit is used for tests and excluded from the runtime artifact.

Redistribution of the connector code does not grant rights to redistribute FXMacroData datasets beyond the applicable data licence. Apache Kafka, Confluent and Redpanda names identify interoperability; no endorsement is implied.
