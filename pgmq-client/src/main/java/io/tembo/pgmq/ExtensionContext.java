package io.tembo.pgmq;

class ExtensionContext {
    private final JsonSerializer serializer;
    private final ConnectionProvider connectionProvider;

    ExtensionContext(JsonSerializer serializer, ConnectionProvider connectionProvider) {
        this.serializer = serializer;
        this.connectionProvider = connectionProvider;
    }

    public JsonSerializer getSerializer() {
        return serializer;
    }

    public ConnectionProvider getConnectionProvider() {
        return connectionProvider;
    }
}
