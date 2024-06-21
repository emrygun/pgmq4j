package io.tembo.pgmq;


import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * FIXME: Rust implementasyonunda read_with_pool yok :/
 */
interface PGMQClient {

    void create(String queue);

    void createUnlogged(String queue);

    /**
     * Destroy a queue. This deletes the queue's tables, indexes and metadata.
     * Does not deletes any data related to adjacent queues.
     * <br>
     * Example:
     * <pre>
     * {@code
     * FIXME: Code example
     * }
     * </pre>
     *
     * @param queueName Queue name.
     */
    void destroy(String queueName);

    MessageId send(String queueName, String message);

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
