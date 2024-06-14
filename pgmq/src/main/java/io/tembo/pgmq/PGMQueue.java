package io.tembo.pgmq;

import org.postgresql.ds.PGConnectionPoolDataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.tembo.pgmq.statement.RawStatement.CREATE;
import static io.tembo.pgmq.statement.RawStatement.CREATE_UNLOGGED;

/**
 * Base class for interacting with a queue.
 */
public class PGMQueue implements PGMQOperations {
    private final PGConnectionPoolDataSource pool;

    public PGMQueue(PGConnectionPoolDataSource pool) throws SQLException {
        this.pool = pool;
    }

    private void createExtensionIfNotPresent(Connection connection) throws SQLException {
        connection
                .prepareStatement("create extension if not exists pgmq cascade;")
                .execute();
    }

    @Override
    public void create(String queueName) throws SQLException {
        try {
            var connection = pool.getConnection();
            connection.setAutoCommit(false);

            for (var statement : initQueueClientOnly(queueName, true)) {
                connection.prepareStatement(statement).execute();
            }
            connection.commit();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createUnlogged(String queueName) throws SQLException {
        try {
            var connection = pool.getConnection();
            connection.setAutoCommit(false);

            for (var statement : initQueueClientOnly(queueName, false)) {
                connection.prepareStatement(statement).execute();
            }
            connection.commit();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void destroy(String queueName) {
        try {
            var connection = pool.getConnection();
            connection.setAutoCommit(false);

            for (var statement : destroyQueueClientOnly(queueName)) {
                connection.prepareStatement(statement).execute();
            }
            connection.commit();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void send(String queueName, String message) {
    }

    @Override
    public Optional<List<PGMQueueMetadata>> listQueues() {
        try {
            ResultSet resultSet = pool.getConnection().prepareStatement("SELECT * from pgmq.list_queues();").executeQuery();
            List<PGMQueueMetadata> list = new ArrayList<>();
            while (resultSet.next()) {
                var metadata = new PGMQueueMetadata(
                        resultSet.getString("queue_name"),
                        resultSet.getTimestamp("created_at").toInstant(),
                        resultSet.getBoolean("is_unlogged"),
                        resultSet.getBoolean("is_partitioned")
                );
                list.add(metadata);
            }

            return Optional.of(list);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
