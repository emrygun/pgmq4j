package io.tembo.pgmq;

import java.sql.SQLException;

/**
 * Factory for creating PGMQueue instances.
 */
public class PGMQueueFactory {
    private final ExtensionContext context;

    /**
     * There is no default serializer option. <br>
     * So you have to bring your own object serializer ¯\_(ツ)_/¯
     *
     * @param serializer JsonSerializer instance for pgmq
     * @param connectionProvider ConnectionProvider instance for pgmq
     */
    public PGMQueueFactory(JsonSerializer serializer, ConnectionProvider connectionProvider) {
        this.context = new ExtensionContext(serializer, connectionProvider);
    }

    /**
     * Create a new queue
     *
     * @return PGMQueue instance
     */
    public PGMQueue create() {
        try {
            return new PGMQueue(context);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
