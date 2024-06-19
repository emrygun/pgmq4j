package io.tembo.pgmq;


import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * FIXME: Rust implementasyonunda read_with_pool yok :/
 */
interface PGMQClient {

    //void new();
    //void newWithPool();

    void create(String queue) throws SQLException;

    void createUnlogged(String queue) throws SQLException;

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

    Integer send(String queueName, String message);

    Integer sendDelay(String queueName, String message, int delaySec);

    List<Integer> sendBatch(String queueName, List<String> messages);


    Optional<DefaultMessage> read(String queueName, int visibilityTime);

    Optional<DefaultMessage> read(String queueName);

    Optional<List<DefaultMessage>> readBatch(String queueName, int visibilityTime, int messageCount);

    Optional<List<DefaultMessage>> readBatchWithPool(String queueName, int visibilityTime, int maxBatchSize, Duration pollTimeout, Duration pollInterval);


    Integer delete(String queueName, int messageId);

    Integer deleteBatch(String queueName, int[] messageIds);


    Integer purge(String queueName);


    Integer archive(String queueName, int messageId);

    Integer archiveBatch(String queueName, int[] messageIds);


    Optional<Message> pop(String queueName);


    Optional<Message> setVisibilityTimeout(String queueName, int messageId, Instant visibilityTimeout);


    Optional<List<PGMQueueMetadata>> listQueues();
}
