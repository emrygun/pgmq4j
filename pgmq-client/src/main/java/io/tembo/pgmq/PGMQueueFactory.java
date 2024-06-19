package io.tembo.pgmq;

import org.postgresql.ds.PGConnectionPoolDataSource;

import java.sql.SQLException;

public class PGMQueueFactory {
    private final ExtensionContext context;

    public PGMQueueFactory(JsonSerializer serializer, PGConnectionPoolDataSource dataSource) {
        this.context = new ExtensionContext(serializer, dataSource);
    }

    public PGMQueue create(String queueName) {
        return create(queueName, false);
    }

    public PGMQueue create(String queueName, boolean logged) {
        try {
            return new PGMQueue(context);
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
