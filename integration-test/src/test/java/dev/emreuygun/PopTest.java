package dev.emreuygun;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.emreuygun.infra.PGMQueueTest;
import dev.emreuygun.pgmq.JsonSerializer;
import dev.emreuygun.pgmq.PGMQueue;
import dev.emreuygun.pgmq.PGMQueueFactory;
import dev.emreuygun.utils.DatabaseUtils;
import dev.emreuygun.utils.PgConnectionProvider;
import dev.emreuygun.utils.SimpleJacksonJsonSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("Queue pop operation tests")
class PopTest extends PGMQueueTest {
    private static final Logger LOG = LoggerFactory.getLogger(PopTest.class);

    record TestMessage(String name, long timestamp) { }

    //Pop a message and check if it is popped.
    @Test
    @DisplayName("Pop messages from a queue and check")
    void popMessage() {
        var pgmq = prepareAndGetPgmq();
        var queueName = "queue_" + UUID.randomUUID().toString().replaceAll("-", "");
        pgmq.create(queueName);

        int messageCount = 5000;
        List<TestMessage> messages = Stream.generate(() -> new TestMessage("test", Instant.now().getEpochSecond()))
                .limit(messageCount)
                .toList();

        pgmq.sendBatch(queueName, messages);

        // Read batch of messages
        for (int i = 0; i < messageCount; i++) {
            var poppedMessage = pgmq.pop(queueName, TestMessage.class);
            var m = poppedMessage.get();
            // Print popped messages
            LOG.info("Message: {}, enqueuedAt: {}, readCount: {}, vt: {}",
                        m.getMessage(), m.getEnqueuedAt(), m.getReadCount(), m.getVisibilityTime());
            assertEquals(m.getMessage(), messages.get(i));
        }

        var poppedMessage = pgmq.pop(queueName, TestMessage.class);
        assertTrue(poppedMessage.isEmpty());
    }

    private PGMQueue prepareAndGetPgmq() {
        // Create json serializer for pgmq
        ObjectMapper objectMapper = new ObjectMapper();
        JsonSerializer jsonSerializer = new SimpleJacksonJsonSerializer(objectMapper);

        // Create a PGMQueueFactory and a basic queue
        PGMQueueFactory pgmqFactory = new PGMQueueFactory(jsonSerializer, new PgConnectionProvider(DatabaseUtils.dataSource));
        return pgmqFactory.create();
    }
}
