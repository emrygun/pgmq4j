package io.tembo.pgmq;


import java.sql.SQLException;

interface PGMQOperations {

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
     * @param queue Queue name.
     */
    void destroy(String queueName);

    void send(String queueName, String message);

    //FIXME: Other operations
}
