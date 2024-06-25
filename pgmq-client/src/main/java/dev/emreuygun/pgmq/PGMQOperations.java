package dev.emreuygun.pgmq;


import java.time.Duration;
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


    Integer delete(String queueName, MessageId messageId);

    Integer deleteBatch(String queueName, List<MessageId> messageIds);


    Integer purge(String queueName);


    Integer archive(String queueName, MessageId messageId);

    Integer archiveBatch(String queueName, List<MessageId> messageIds);


    <T> Optional<Message<T>> pop(String queueName, Class<T> clazz);


    <T> Optional<Message<T>> setVisibilityTimeout(String queueName, MessageId messageId, Duration visibilityTimeout, Class<T> clazz);


    List<PGMQueueMetadata> listQueues();
}
