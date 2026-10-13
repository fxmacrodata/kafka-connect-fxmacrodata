-- Materialize the Kafka JSON-converter envelopes with your normal SQL sink.
-- Project payload.operation, payload.currency and the other Struct fields.
-- Preserve record_json and response_json for exact timestamps, revisions and provenance.
CREATE VIEW usd_release_context AS
SELECT currency, indicator, announcement_datetime, value_text, record_json, response_json
FROM fxmacrodata_usd_releases
WHERE currency = 'usd';

SELECT indicator, value_text, announcement_datetime, record_json
FROM usd_release_context
WHERE announcement_datetime IS NOT NULL;

-- For an event-window or as-of join, interpret the original operation's timestamp
-- units and publication-time status before converting announcement_datetime.
-- A release period is not its publication time, and unavailable publication times
-- must remain unavailable.
