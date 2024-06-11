package io.tembo.pgmq;

import io.tembo.pgmq.statement.RawStatement;
import org.postgresql.ds.PGConnectionPoolDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static io.tembo.pgmq.statement.RawStatement.CREATE;
import static io.tembo.pgmq.statement.RawStatement.CREATE_UNLOGGED;

/**
 * Base class for interacting with a queue.
 */
public class PGMQueue implements PGMQOperations {
    private final Integer delay = 0;
    private final Integer vt = 0;
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
        var statement = pool.getConnection().prepareStatement(CREATE.getStatement());
        statement.setString(1, queueName);
        statement.execute();
    }

    @Override
    public void createUnlogged(String queueName) throws SQLException {
        var statement = pool.getConnection().prepareStatement(CREATE_UNLOGGED.getStatement());
        statement.setString(1, queueName);
        statement.execute();
    }

    @Override
    public void destroy(String queueName) {
    }

    @Override
    public void send(String queueName, String message) {
    }
}
