package io.tembo.pgmq;


import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * FIXME: Rust implementasyonunda read_with_pool yok :/
 */
interface PGMQOperations {


    void create() ;

    void createUnlogged() ;

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
     */
    void destroy();

    MessageId send(String message);

    MessageId sendDelay(String message, int delaySec);

    List<Integer> sendBatch(List<String> messages);


    Optional<Message> read(int visibilityTime);

    Optional<Message> read();

    Optional<List<Message>> readBatch(int visibilityTime, int messageCount);

    Optional<List<Message>> readBatchWithPool(int visibilityTime, int maxBatchSize, Duration pollTimeout, Duration pollInterval);


    MessageId delete(MessageId messageId);

    MessageId deleteBatch(MessageId[] messageIds);


    MessageId purge();


    MessageId archive(MessageId messageId);

    MessageId archiveBatch(MessageId[] messageIds);


    Optional<Message> pop();


    Optional<Message> setVisibilityTimeout(int messageId, Instant visibilityTimeout);


    Optional<List<PGMQueueMetadata>> listQueues();
}
