# FXMacroData Kafka Connect source

A native `SourceConnector` for loading FXMacroData records into Kafka topics. The public USD catalogue, indicator-history and release-calendar examples require no API key or account.

[FXMacroData API reference](https://fxmacrodata.com/documentation/reference?utm_source=github&utm_medium=referral&utm_campaign=kafka-connect-fxmacrodata&utm_content=readme)

## Build and install

Use Java 21 and Maven 3.9 on your build machine:

```sh
mvn verify
```

The build produces a shaded connector JAR and `fxmacrodata-kafka-connect-fxmacrodata-0.1.0.zip` under `target/`. Extract the ZIP into a dedicated child directory of your worker's `plugin.path`, then restart that worker through your usual administration process. `GET /connector-plugins` should list `com.fxmacrodata.kafka.FxMacroDataSourceConnector`. Submit `examples/usd-catalogue.json` to the worker's `POST /connectors` endpoint.

The connector targets Kafka Connect 4.1.2 APIs. Check worker compatibility and installation before production use. A Confluent-format archive does not imply Marketplace approval or a Confluent Cloud managed connector.

## Streams and credentials

`streams` is a JSON array with `operation`, `arguments` and `topic` per stream. Multiple streams distribute across `tasks.max`; each stream is assigned to one task. Print machine-readable operation schemas with `java -cp target/kafka-connect-fxmacrodata-0.1.0.jar com.fxmacrodata.kafka.OperationCatalogue`. The shipped operation catalogues are also available as JAR resources under `com/fxmacrodata/client/`. Their input schemas define the supported arguments. MCP tools accept either `mcp_<tool_name>` or `mcp:<tool_name>`.

The default `api.key.env` is empty, which selects anonymous access. For optional protected operations, inject a secret into the worker process environment and set `api.key.env` to its variable **name**, for example `FXMD_API_KEY`. The credential is resolved inside each task, used in HTTP headers, and never put into task configurations or offsets. Do not put credentials in `streams`, connector JSON, URLs or arguments. Fixed HTTPS origins and disabled redirects prevent forwarding credentials to arbitrary hosts.

| Configuration | Default | Purpose |
| --- | --- | --- |
| `poll.interval.ms` | 60000 | Minimum interval between snapshots of each stream |
| `batch.size` | 500 | Maximum records in a Kafka Connect poll |
| `max.pages` | 1000 | Complete-history pagination budget |
| `max.records` | 100000 | Maximum records retained per snapshot |
| `max.snapshot.bytes` | 8388608 | Maximum serialized original response retained per emitted record |
| `request.timeout.seconds` | 30 | HTTP/SSE request deadline, at most 120 seconds |
| `api.key.env` | empty | Optional environment variable name |

## Records and recovery

The typed Kafka Connect `Struct` supplies `operation`, credential-free `source_url`, `record_id`, `snapshot_id`, currency/indicator/date/time views, `value`, `value_text`, `record_json`, and `response_json`. The optional floating-point `value` is an analytical view; `value_text` and original JSON retain exact numeric meaning. JSON fields preserve original product fields, revisions, provenance, nested records and metadata. Empty responses emit one metadata envelope with a null `record_json`. Dates stay as supplied; the connector never estimates release times.

Offset-paginated REST operations and cursor-paginated calendar REST/MCP operations read complete history within `max.pages`, retaining every original page and pinning advertised content versions or calendar checkpoints. Other MCP tools retain their full bounded tool response, including visual-artifact output; an offset used to select a chart window is not treated as an export cursor. A changing content version, invalid pagination or exhausted page budget fails the snapshot instead of silently truncating it. Other REST and MCP operations retain the full response of their bounded call. `stream_events` uses a finite SSE capture configured through its published `max_seconds` and `max_events` arguments.

Kafka Connect commits offsets after output acknowledgement. Offsets contain stream identity, a snapshot fingerprint and the emitted row index. On restart, an unchanged snapshot resumes after the committed row; a changed snapshot replays from its beginning so revisions are not skipped. Identical snapshots are suppressed during normal polling. At-least-once delivery can produce duplicates after failure or changed snapshots; downstream consumers can use `record_id` for idempotency. A mutable polling API is not an immutable event log, and transient updates that disappear between polls cannot be recovered by this connector. Exactly-once support is explicitly disabled.

Every record retains the original response, so the Kafka message can be as large as that response. Coordinate worker producer `max.request.size` and topic/broker `max.message.bytes` with the selected response budget, or narrow the query window. The connector fails before emission when its explicit byte budget is exceeded.

Use history/version/change operations where the public contract offers them to retain revisions. A topic configured for compaction by `record_id` represents unique record content; retain the original log if repeated identical occurrences matter.

## Capability coverage

All 33 REST operations in `operations.json` and all 54 tools in `mcp-tools.json` can be configured as native streams. This includes discovery, macro history and releases, calendar snapshots and event versions, FX/reference/intraday/session rates, market sessions, curves and differentials, financial prices, COT, commodities, press releases, risk sentiment and changes, factors, forecasts, coverage, research panels, announcement changes and bounded SSE. Access depends on the operation's public contract and the customer-supplied key. Each capability is delivered as a Kafka `SourceRecord`, with a stable typed envelope and the full response. `CAPABILITIES.md` lists each supported operation.

`examples/usd-history-and-calendar.json` loads macro releases and release-calendar records into separate topics. `examples/event-context.sql` demonstrates an event-context view after a Kafka-to-SQL sink materializes the envelopes.

Tests use synthetic offline responses and exercise task distribution, lossless envelopes, restart offsets, changed snapshots, bounds and credential-safe configuration. They never contact FXMacroData.
