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

    private final String queueName;
    private final PGMQClient client;

    private final PGConnectionPoolDataSource pool;
    private final JsonSerializer jsonSerializer;


    PGMQueue(String queueName, ExtensionContext context) throws SQLException {
        this.queueName = queueName;
        this.pool = context.getPoolDataSource();
        this.jsonSerializer = context.getSerializer();
        this.client = new DefaultPGMQClient((PgConnection) pool.getConnection());

        createExtensionIfNotPresent(pool.getConnection());
    }

    private void createExtensionIfNotPresent(Connection connection) throws SQLException {
        connection
                .prepareStatement("create extension if not exists pgmq cascade;")
                .execute();
    }

    @Override
    public void create() throws SQLException {
        client.create(queueName);
    }

    @Override
    public void createUnlogged() throws SQLException {
        client.createUnlogged(queueName);
    }

    @Override
    public void destroy() {
        client.destroy(queueName);
    }

    @Override
    public Integer send(String message) {
        return sendDelay(message, 0);
    }

    @Override
    public Integer sendDelay(String message, int delaySec) {
        return client.sendDelay(queueName, message, delaySec);
    }

    @Override
    public List<Integer> sendBatch(List<String> messages) {
        return client.sendBatch(queueName, messages);
    }

    @Override
    public Optional<List<PGMQueueMetadata>> listQueues() {
        return client.listQueues();
    }

    @Override
    public Optional<DefaultMessage> read(int visibilityTime) {
        return client.read(queueName, visibilityTime);
    }

    @Override
    public Optional<DefaultMessage> read() {
        return read(30);
    }

    @Override
    public Optional<List<DefaultMessage>> readBatch(int visibilityTime, int messageCount) {
        return client.readBatch(queueName, visibilityTime, messageCount);
    }

    @Override
    public Optional<List<DefaultMessage>> readBatchWithPool(int visibilityTime, int maxBatchSize, Duration pollTimeout, Duration pollInterval) {
        //FIXME: Implementation
        return Optional.empty();
    }

    @Override
    public Integer delete(int messageId) {
        return client.delete(queueName, messageId);
    }

    @Override
    public Integer deleteBatch(int[] messageIds) {
        return client.deleteBatch(queueName, messageIds);
    }

    @Override
    public Integer purge() {
        return client.purge(queueName);
    }

    @Override
    public Integer archive(int messageId) {
        return archiveBatch(new int[] {messageId});
    }

    @Override
    public Integer archiveBatch(int[] messageIds) {
        return client.archiveBatch(queueName, messageIds);
    }

    @Override
    public Optional<Message> pop() {
        return client.pop(queueName);
    }

    @Override
    public Optional<Message> setVisibilityTimeout(int messageId, Instant visibilityTimeout) {
        return null; //FIXME: Implementation is missing
    }

    public String getQueueName() {
        return queueName;
    }
}
