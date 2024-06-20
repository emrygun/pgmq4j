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
    public void create() {
        client.create(queueName);
    }

    @Override
    public void createUnlogged() {
        client.createUnlogged(queueName);
    }

    @Override
    public void destroy() {
        client.destroy(queueName);
    }

    @Override
    public MessageId send(String message) {
        return sendDelay(message, 0);
    }

    @Override
    public MessageId sendDelay(String message, int delaySec) {
        return client.sendDelay(queueName, message, delaySec);
    }

    @Override
    public List<MessageId> sendBatch(List<String> messages) {
        return client.sendBatch(queueName, messages);
    }

    @Override
    public Optional<List<PGMQueueMetadata>> listQueues() {
        return client.listQueues();
    }

    @Override
    public Optional<Message> read(int visibilityTime) {
        return client.read(queueName, visibilityTime);
    }

    @Override
    public Optional<Message> read() {
        return read(30);
    }

    @Override
    public Optional<List<Message>> readBatch(int visibilityTime, int messageCount) {
        return client.readBatch(queueName, visibilityTime, messageCount);
    }

    @Override
    public Optional<List<Message>> readBatchWithPool(int visibilityTime, int maxBatchSize, Duration pollTimeout, Duration pollInterval) {
        //FIXME: Implementation
        return Optional.empty();
    }

    @Override
    public MessageId delete(MessageId messageId) {
        return client.delete(queueName, messageId);
    }

    @Override
    public MessageId deleteBatch(MessageId[] messageIds) {
        return client.deleteBatch(queueName, messageIds);
    }

    @Override
    public MessageId purge() {
        return client.purge(queueName);
    }

    @Override
    public MessageId archive(int messageId) {
        return archiveBatch(new int[] {messageId});
    }

    @Override
    public MessageId archiveBatch(MessageId[] messageIds) {
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
