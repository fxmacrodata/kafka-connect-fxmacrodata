package com.fxmacrodata.kafka;

import com.fxmacrodata.client.Result;

/** Injection boundary for offline tests and application-owned clients. */
interface SnapshotReader extends AutoCloseable {
    Result read(StreamSpec stream, int maxPages);
    @Override void close();
}
