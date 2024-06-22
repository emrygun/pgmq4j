package dev.emreuygun.pgmq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
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
        this.client = new DefaultPGMQClient(context.getConnectionProvider().getConnection());

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
    public <T> MessageId sendDelay(String queueName, T message, int delaySec) {
        return client.sendDelay(queueName, jsonSerializer.toJson(message), delaySec);
    }

    @Override
    public <T> List<MessageId> sendBatch(String queueName, List<T> messages) {
        return client.sendBatch(queueName, messages.stream().map(jsonSerializer::toJson).toList());
    }

    // Read operations

    @Override
    public Reader<?> read(String queueName) {
        return new Reader<String>(this, this.client, queueName);
    }

    /*
    @Override
    public Optional<List<AbstractMessage>> readBatchWithPool(String queueName, int visibilityTime, int maxBatchSize, Duration pollTimeout, Duration pollInterval) {
        //FIXME: Implementation
        return Optional.empty();
    }
     */

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
    public <T> Optional<Message<T>> pop(String queueName, Class<T> clazz) {
        return null; //FIXME: Implementation is missing
        // return client.pop(queueName);
    }

    @Override
    public <T> Optional<Message<T>> setVisibilityTimeout(String queueName, MessageId messageId, Instant visibilityTimeout, Class<T> clazz) {
        return null; //FIXME: Implementation is missing
    }

    @Override
    public Optional<List<PGMQueueMetadata>> listQueues() {
        return client.listQueues();
    }

    JsonSerializer getJsonSerializer() {
        return jsonSerializer;
    }
}