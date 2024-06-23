package dev.emreuygun.pgmq;


import java.time.Instant;
import java.util.List;
import java.util.Optional;

interface PGMQClient {

    void create(String queue);

    void createUnlogged(String queue);

    /**
     * Destroy a queue. This deletes the queue's tables, indexes and metadata.
     * Does not deletes any data related to adjacent queues.
     * @param queueName Queue name.
     */
    void destroy(String queueName);


    MessageId send(String queueName, String message, int delaySec);

    List<MessageId> sendBatch(String queueName, List<String> messages);


    Optional<ByteArrayMessage> read(String queueName, int visibilityTime);

    List<ByteArrayMessage> readBatch(String queueName, int visibilityTime, int messageCount);



    Integer delete(String queueName, MessageId messageId);

    Integer deleteBatch(String queueName, List<MessageId> messageIds);


    Integer purge(String queueName);


    Integer archive(String queueName, List<MessageId> messageIds);


    Optional<ByteArrayMessage> pop(String queueName);


    Optional<ByteArrayMessage> setVisibilityTimeout(String queueName, MessageId messageId, Instant visibilityTimeout);


    Optional<List<PGMQueueMetadata>> listQueues();
}
