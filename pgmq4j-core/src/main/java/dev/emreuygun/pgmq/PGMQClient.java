package dev.emreuygun.pgmq;


import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

interface PGMQClient {

    void create(String queue);

    void createUnlogged(String queue);


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


    Optional<ByteArrayMessage> setVisibilityTimeout(String queueName, MessageId messageId, Duration visibilityTimeout);


    List<PGMQueueMetadata> listQueues();
}
