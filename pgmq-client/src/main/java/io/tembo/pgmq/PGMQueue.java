package io.tembo.pgmq;

import org.postgresql.jdbc.PgConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Base class for interacting with a queue.
 */
public class PGMQueue implements PGMQOperations {
    private static final Logger LOG = LoggerFactory.getLogger(PGMQueue.class);

    private final PGMQClient client;

    private final JsonSerializer jsonSerializer;
    private final ExtensionContext context;

    PGMQueue(ExtensionContext context) throws SQLException {
        this.context = context;
        this.jsonSerializer = context.getSerializer();
        this.client = new DefaultPGMQClient((PgConnection) context.getConnectionProvider().getConnection());

        createExtensionIfNotPresent(context.getConnectionProvider().getConnection());
    }

    private void createExtensionIfNotPresent(Connection connection) throws SQLException {
        connection
                .prepareStatement("create extension if not exists pgmq cascade;")
                .execute();
    }

    @Override
    public void create(String queueName) {
        client.create(queueName);
    }

    @Override
    public void createUnlogged(String queueName) {
        client.createUnlogged(queueName);
    }

    @Override
    public void destroy(String queueName) {
        client.destroy(queueName);
    }

    // Send operations

    @Override
    public <T> MessageId send(String queueName, T message) {
        return sendDelay(queueName, message, 0);
    }

    @Override
    public MessageId sendDelay(String queueName, String message, int delaySec) {
        return client.sendDelay(queueName, message, delaySec);
    }

    @Override
    public List<MessageId> sendBatch(String queueName, List<String> messages) {
        return client.sendBatch(queueName, messages);
    }

    // Read operations

    @Override
    public Optional<Message> read(String queueName, int visibilityTime) {
        return client.read(queueName, visibilityTime);
    }

    @Override
    public Optional<Message> read(String queueName) {
        return read(queueName, 30);
    }

    @Override
    public Optional<List<Message>> readBatch(String queueName, int visibilityTime, int messageCount) {
        return client.readBatch(queueName, visibilityTime, messageCount);
    }

    @Override
    public Optional<List<Message>> readBatchWithPool(String queueName, int visibilityTime, int maxBatchSize, Duration pollTimeout, Duration pollInterval) {
        //FIXME: Implementation
        return Optional.empty();
    }

    @Override
    public Integer delete(String queueName, MessageId messageId) {
        return client.delete(queueName, messageId);
    }

    @Override
    public Integer deleteBatch(String queueName, List<MessageId> messageIds) {
        return client.deleteBatch(queueName, messageIds);
    }

    @Override
    public Integer purge(String queueName) {
        return client.purge(queueName);
    }

    @Override
    public Integer archive(String queueName, MessageId messageId) {
        return archiveBatch(queueName, Collections.singletonList(messageId));
    }

    @Override
    public Integer archiveBatch(String queueName, List<MessageId> messageIds) {
        return client.archiveBatch(queueName, messageIds);
    }

    @Override
    public Optional<Message> pop(String queueName) {
        return client.pop(queueName);
    }

    @Override
    public Optional<Message> setVisibilityTimeout(String queueName, MessageId messageId, Instant visibilityTimeout) {
        return null; //FIXME: Implementation is missing
    }

    @Override
    public Optional<List<PGMQueueMetadata>> listQueues() {
        return client.listQueues();
    }

}
