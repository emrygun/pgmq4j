package io.tembo.pgmq;

import org.postgresql.ds.PGConnectionPoolDataSource;

class ExtensionContext {
    private final JsonSerializer serializer;
    private final PGConnectionPoolDataSource poolDataSource;

    ExtensionContext(JsonSerializer serializer, PGConnectionPoolDataSource poolDataSource) {
        this.serializer = serializer;
        this.poolDataSource = poolDataSource;
    }

    public JsonSerializer getSerializer() {
        return serializer;
    }

    public PGConnectionPoolDataSource getPoolDataSource() {
        return poolDataSource;
    }
}
