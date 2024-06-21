package io.tembo.pgmq;


import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * FIXME: Rust implementasyonunda read_with_pool yok :/
 */
interface PGMQOperations {


    void create(String queueName) ;

    void createUnlogged(String queueName) ;

    void destroy(String queueName);


    <T> MessageId send(String queueName, T message);

    MessageId sendDelay(String queueName, String message, int delaySec);

    List<MessageId> sendBatch(String queueName, List<String> messages);


    Optional<Message> read(String queueName, int visibilityTime);

    Optional<Message> read(String queueName);

    Optional<List<Message>> readBatch(String queueName, int visibilityTime, int messageCount);

    Optional<List<Message>> readBatchWithPool(String queueName, int visibilityTime, int maxBatchSize, Duration pollTimeout, Duration pollInterval);


    Integer delete(String queueName, MessageId messageId);

    Integer deleteBatch(String queueName, List<MessageId> messageIds);


    Integer purge(String queueName);


    Integer archive(String queueName, MessageId messageId);

    Integer archiveBatch(String queueName, List<MessageId> messageIds);


    Optional<Message> pop(String queueName);


    Optional<Message> setVisibilityTimeout(String queueName, MessageId messageId, Instant visibilityTimeout);


    Optional<List<PGMQueueMetadata>> listQueues();
}
