package org.example.pgmq.basic;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.emreuygun.pgmq.PGMQueueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    private static final String QUEUE_BASIC = "my_basic_queue";
    private static final Integer VISIBILITY_TIMEOUT_SEC = 30;

    record TestMessage(String name, long timestamp) { }

    /**
     * This is a simple example of how to use the pgmq library.
     */
    public static void main(String[] args) {
        DatabaseUtils.postgresContainer.start();
        DatabaseUtils.dataSource.setURL(DatabaseUtils.postgresContainer.getJdbcUrl());

        // Create json serializer for pgmq
        var objectMapper = new ObjectMapper();
        var jsonSerializer = new SimpleJacksonJsonSerializer(objectMapper);

        // Create a PGMQueueFactory and a basic queue
        var pgmqFactory = new PGMQueueFactory(jsonSerializer, new PgConnectionProvider(DatabaseUtils.dataSource));
        var pgmq = pgmqFactory.create();
        pgmq.create(QUEUE_BASIC);

        // Send and read a message test
        TestMessage testMessage = new TestMessage("test", Instant.now().getEpochSecond());
        pgmq.send(QUEUE_BASIC, testMessage);
        var message = pgmq.read(QUEUE_BASIC)
                .as(TestMessage.class)
                .visibilityTime(VISIBILITY_TIMEOUT_SEC)
                .oneValue();

        assert message.isPresent();
        message.ifPresent(m -> LOG.info("Message: {}, enqueuedAt: {}, readCount: {}, vt: {}",
                m.getMessage(),
                m.getEnqueuedAt(),
                m.getReadCount(),
                m.getVisibilityTime())
        );

        // Send multiple messages and read values
        pgmq.send(QUEUE_BASIC, testMessage);
        pgmq.send(QUEUE_BASIC, testMessage);
        pgmq.send(QUEUE_BASIC, testMessage);
        pgmq.send(QUEUE_BASIC, testMessage);

        pgmq.read(QUEUE_BASIC)
                .as(TestMessage.class)
                .visibilityTime(VISIBILITY_TIMEOUT_SEC)
                .polling()
                .values(10)
                .forEach(m ->
                    LOG.info("Message: {}, enqueuedAt: {}, readCount: {}, vt: {}",
                        m.getMessage(),
                        m.getEnqueuedAt(),
                        m.getReadCount(),
                        m.getVisibilityTime())
                );

        DatabaseUtils.postgresContainer.stop();
    }
}