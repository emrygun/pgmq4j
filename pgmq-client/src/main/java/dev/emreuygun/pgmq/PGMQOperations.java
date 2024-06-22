package dev.emreuygun.pgmq;


import java.time.Instant;
import java.util.List;
import java.util.Optional;

interface PGMQOperations {

    void create(String queueName);

    void createUnlogged(String queueName);

    void destroy(String queueName);


    <T> MessageId send(String queueName, T message);

    <T> MessageId sendDelay(String queueName, T message, int delaySec);

    <T> List<MessageId> sendBatch(String queueName, List<T> messages);


    Reader<?> read(String queueName);

    // Optional<AbstractMessage> read(String queueName);

    // Optional<List<AbstractMessage>> readBatch(String queueName, int visibilityTime, int messageCount);

    // Optional<List<AbstractMessage>> readBatchWithPool(String queueName, int visibilityTime, int maxBatchSize, Duration pollTimeout, Duration pollInterval);


    Integer delete(String queueName, MessageId messageId);

    Integer deleteBatch(String queueName, List<MessageId> messageIds);


    Integer purge(String queueName);


    Integer archive(String queueName, MessageId messageId);

    Integer archiveBatch(String queueName, List<MessageId> messageIds);


    <T> Optional<Message<T>> pop(String queueName, Class<T> clazz);


    <T> Optional<Message<T>> setVisibilityTimeout(String queueName, MessageId messageId, Instant visibilityTimeout, Class<T> clazz);


    Optional<List<PGMQueueMetadata>> listQueues();
}
