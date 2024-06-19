package io.tembo.pgmq;


import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * FIXME: Rust implementasyonunda read_with_pool yok :/
 */
interface PGMQOperations {

    //void new();
    //void newWithPool();

    void create() throws SQLException;

    void createUnlogged() throws SQLException;

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

    Integer send(String message);

    Integer sendDelay(String message, int delaySec);

    List<Integer> sendBatch(List<String> messages);


    Optional<DefaultMessage> read(int visibilityTime);

    Optional<DefaultMessage> read();

    Optional<List<DefaultMessage>> readBatch(int visibilityTime, int messageCount);

    Optional<List<DefaultMessage>> readBatchWithPool(int visibilityTime, int maxBatchSize, Duration pollTimeout, Duration pollInterval);


    Integer delete(int messageId);

    Integer deleteBatch(int[] messageIds);


    Integer purge();


    Integer archive(int messageId);

    Integer archiveBatch(int[] messageIds);


    Optional<Message> pop();


    Optional<Message> setVisibilityTimeout(int messageId, Instant visibilityTimeout);


    Optional<List<PGMQueueMetadata>> listQueues();
}
