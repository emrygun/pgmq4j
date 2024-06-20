package io.tembo.pgmq;

import org.postgresql.ds.PGConnectionPoolDataSource;

import java.sql.SQLException;

public class PGMQueueFactory {
    private final ExtensionContext context;

    /**
     * There is no default serializer option. <br>
     * So you have to bring your own object serializer ¯\_(ツ)_/¯
     *
     * @param serializer JsonSerializer instance for pgmq
     * @param dataSource Pool datasource. Bring your own pool
     */
    public PGMQueueFactory(JsonSerializer serializer, PGConnectionPoolDataSource dataSource) {
        this.context = new ExtensionContext(serializer, dataSource);
    }

    public PGMQueue create(String queueName) {
        return create(queueName, false);
    }

    public PGMQueue create(String queueName, boolean logged) {
        try {
            var queue = new PGMQueue(queueName, context);
            if (logged) {
                queue.create();
            } else {
                queue.createUnlogged();
            }

            return queue;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public PGMQueue create(String jdbcUrl, String queueName) {
        return create(jdbcUrl, queueName, false);
    }

    public PGMQueue create(String jdbcUrl, String queueName, boolean logged) {
        return null;
    }
}
