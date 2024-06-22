package io.tembo.pgmq.example.basic;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.tembo.pgmq.PGMQueueFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

import static io.tembo.pgmq.example.basic.DatabaseUtils.dataSource;
import static io.tembo.pgmq.example.basic.DatabaseUtils.postgresContainer;

public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    private static final String QUEUE_BASIC = "my_basic_queue";
    private static final Integer VISIBILITY_TIMEOUT_SEC = 30;

    record TestMessage(String name, long timestamp) { }

    /**
     * This is a simple example of how to use the pgmq library.
     */
    public static void main(String[] args) {
        postgresContainer.start();
        dataSource.setURL(postgresContainer.getJdbcUrl());

        // Create json serializer for pgmq
        var objectMapper = new ObjectMapper();
        var jsonSerializer = new SimpleJacksonJsonSerializer(objectMapper);

        // Create a PGMQueueFactory and a basic queue
        var pgmqFactory = new PGMQueueFactory(jsonSerializer, new PgConnectionProvider(dataSource));
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

        postgresContainer.stop();
    }
}