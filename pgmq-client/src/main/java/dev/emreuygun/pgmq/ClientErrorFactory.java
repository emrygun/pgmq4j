package dev.emreuygun.pgmq;

import java.time.Duration;

public class ClientErrorFactory {
    static PGMQError createQueueError(String queueName, boolean logged, Throwable ex) {
        return new PGMQError(
                "PGMQ_ERR_001",
                "Error while creating queue. Queue: %s, Logged: %s.".formatted(queueName, logged),
                ex
        );
    }

    static PGMQError destroyQueueError(String queueName, Throwable ex) {
        return new PGMQError(
                "PGMQ_ERR_002",
                "Error while destroying queue. Queue: %s.".formatted(queueName),
                ex
        );
    }

    static PGMQError sendMessageError(int delaySec, int messageCount, Throwable ex) {
        return new PGMQError(
                "PGMQ_ERR_003",
                "Error while sending message. Delay: %d seconds, AbstractMessage count: %d".formatted(delaySec, messageCount),
                ex
        );
    }

    static PGMQError readMessageError(String queueName, int visibilityTime, int limit, Throwable ex) {
        return new PGMQError(
                "PGMQ_ERR_004",
                "Error while reading message from queue. Queue: %s, vt: %d, limit: %d.".formatted(queueName, visibilityTime, limit),
                ex
        );
    }

    static PGMQError listQueuesError(Throwable ex) {
        return new PGMQError(
                "PGMQ_ERR_005",
                "Error while listing queues.",
                ex
        );
    }

    static PGMQError deleteMessageError(Throwable ex) {
        return new PGMQError(
                "PGMQ_ERR_006",
                "Error while deleting messages.",
                ex
        );
    }

    static PGMQError purgeError(String queueName, Throwable ex) {
        return new PGMQError(
                "PGMQ_ERR_007",
                "Error while purge queue. Queue: %s.".formatted(queueName),
                ex
        );
    }

    static PGMQError archiveError(String queueName, Throwable ex) {
        return new PGMQError(
                "PGMQ_ERR_008",
                "Error archiving queue. Queue: %s.".formatted(queueName),
                ex
        );
    }

    static PGMQError popError(String queueName, Throwable ex) {
        return new PGMQError(
                "PGMQ_ERR_009",
                "Error while popping message from queue. Queue: %s.".formatted(queueName),
                ex
        );
    }

    static PGMQError setVisibilityTimeoutError(String queueName, MessageId messageId, Duration visibilityTimeout, Throwable ex) {
        return new PGMQError(
                "PGMQ_ERR_010",
                "Error while setting visibility timeout. Queue: %s, MessageId: %s, VisibilityTimeout: %s.".formatted(queueName, messageId, visibilityTimeout),
                ex
        );
    }
}
