package io.tembo.pgmq;

import org.postgresql.ds.PGConnectionPoolDataSource;
import org.postgresql.jdbc.PgConnection;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;

/**
 * Base class for interacting with a queue.
 */
public class PGMQueue implements PGMQOperations {
    private static final Logger LOG = Logger.getLogger(PGMQueue.class.getName());

    private final PGConnectionPoolDataSource pool;
    private final JsonSerializer jsonSerializer;
    private final PGMQClient client;

    PGMQueue(ExtensionContext context) throws SQLException {
        this.pool = context.getPoolDataSource();
        this.jsonSerializer = context.getSerializer();
        this.client = new PGMQClient((PgConnection) pool.getConnection());

        createExtensionIfNotPresent(pool.getConnection());
    }

    private void createExtensionIfNotPresent(Connection connection) throws SQLException {
        connection
                .prepareStatement("create extension if not exists pgmq cascade;")
                .execute();
    }

    @Override
    public void create(String queueName) throws SQLException {
        client.create(queueName);
    }

    @Override
    public void createUnlogged(String queueName) throws SQLException {
        client.createUnlogged(queueName);
    }

    @Override
    public void destroy(String queueName) {
        client.destroy(queueName);
    }

    @Override
    public Integer send(String queueName, String message) {
        return sendDelay(queueName, message, 0);
    }

    @Override
    public Integer sendDelay(String queueName, String message, int delaySec) {
        return client.sendDelay(queueName, message, delaySec);
    }

    @Override
    public List<Integer> sendBatch(String queueName, List<String> messages) {
        return client.sendBatch(queueName, messages);
    }

    @Override
    public Optional<List<PGMQueueMetadata>> listQueues() {
        return client.listQueues();
    }

    @Override
    public Optional<DefaultMessage> read(String queueName, int visibilityTime) {
        return client.read(queueName, visibilityTime);
    }

    @Override
    public Optional<DefaultMessage> read(String queueName) {
        return read(queueName, 30);
    }

    @Override
    public Optional<List<DefaultMessage>> readBatch(String queueName, int visibilityTime, int messageCount) {
        return client.readBatch(queueName, visibilityTime, messageCount);
    }

    @Override
    public Optional<List<DefaultMessage>> readBatchWithPool(String queueName, int visibilityTime, int maxBatchSize, Duration pollTimeout, Duration pollInterval) {
        //FIXME: Implementation
        return Optional.empty();
    }

    @Override
    public Integer delete(String queueName, int messageId) {
        return client.delete(queueName, messageId);
    }

    @Override
    public Integer deleteBatch(String queueName, int[] messageIds) {
        return client.deleteBatch(queueName, messageIds);
    }

    @Override
    public Integer purge(String queueName) {
        return client.purge(queueName);
    }

    @Override
    public Integer archive(String queueName, int messageId) {
        return archiveBatch(queueName, new int[] {messageId});
    }

    @Override
    public Integer archiveBatch(String queueName, int[] messageIds) {
        return client.archiveBatch(queueName, messageIds);
    }

    @Override
    public Optional<Message> pop(String queueName) {
        return client.pop(queueName);
    }

    @Override
    public Optional<Message> setVisibilityTimeout(String queueName, int messageId, Instant visibilityTimeout) {
        return null; //FIXME: Implementation is missing
    }
}
