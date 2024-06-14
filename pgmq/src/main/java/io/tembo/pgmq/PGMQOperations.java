package io.tembo.pgmq;


import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

interface PGMQOperations extends PGMQClient {

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
     * Set<String> s
     * }
     * </pre>
     *
     * @param queueName Queue name.
     */
    void destroy(String queueName);

    void send(String queueName, String message);

    Optional<List<PGMQueueMetadata>> listQueues();

    //FIXME: Other operations
}
