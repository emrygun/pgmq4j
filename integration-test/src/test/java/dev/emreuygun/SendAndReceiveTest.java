package dev.emreuygun;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.emreuygun.infra.TestBase;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SendAndReceiveTest extends TestBase {
    private static final Logger LOG = LoggerFactory.getLogger(SendAndReceiveTest.class);

    record TestMessage(String name, long timestamp) { }

    String QUEUE_BASIC = "my_basic_queue";
    int VISIBILITY_TIMEOUT_SEC = 30;

    @Test
    @DisplayName("Send and receive a single message")
    void sendAndReceiveMessage() {
        var pgmq = prepareAndGetPgmq();
        pgmq.create(QUEUE_BASIC);

        // Send and read a message test
        var messageToSend = new TestMessage("test", Instant.now().getEpochSecond());

        pgmq.send(QUEUE_BASIC, messageToSend);
        var message = pgmq.read(QUEUE_BASIC)
                .as(TestMessage.class)
                .visibilityTime(VISIBILITY_TIMEOUT_SEC)
                .oneValue();

        // Check if the message is present and the message is the same as the one sent
        assertTrue(message::isPresent);
        assertEquals(message.get().getMessage(), messageToSend);

        message.ifPresent(m -> LOG.info("Message: {}, enqueuedAt: {}, readCount: {}, vt: {}",
                m.getMessage(),
                m.getEnqueuedAt(),
                m.getReadCount(),
                m.getVisibilityTime())
        );
    }

    @Test
    @DisplayName("Send and receive a multiple messages in batch")
    void sendAndReceiveMultipleMessages() {
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
