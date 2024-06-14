package io.tembo.pgmq;

import java.util.List;

public interface PGMQClient {

    default List<String> initQueueClientOnly(String queueName, boolean isUnlogged) {
        return List.of(
            createSchema(),
            createMeta(),
            createQueue(queueName, isUnlogged),
            createIndex(queueName),
            createArchive(queueName),
            createArchiveIndex(queueName)
            //insertMeta(name, false, is_unlogged),
            //grantPgmonMeta(),
            //grantPgmonQueue(name)
        );
    }

    default List<String> destroyQueueClientOnly(String queueName) {
        return List.of(
                createSchema(),
                dropQueue(queueName),
                dropQueueArchive(queueName),
                deleteQueueMetadata(queueName)
        );
    }

    String deleteQueueMetadata(String queueName);

    String dropQueueArchive(String queueName);

    String dropQueue(String queueName);

    String createMeta();

    String createSchema();

    String createQueue(String queueName, boolean isUnlogged);

    String createIndex(String queueName);

    String createArchive(String queueName);

    String createArchiveIndex(String queueName);

}
