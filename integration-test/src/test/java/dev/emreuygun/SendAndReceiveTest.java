package dev.emreuygun;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.emreuygun.infra.PGMQueueTest;
import dev.emreuygun.pgmq.JsonSerializer;
import dev.emreuygun.pgmq.Message;
import dev.emreuygun.pgmq.PGMQueue;
import dev.emreuygun.pgmq.PGMQueueFactory;
import dev.emreuygun.utils.DatabaseUtils;
import dev.emreuygun.utils.PgConnectionProvider;
import dev.emreuygun.utils.SimpleJacksonJsonSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Deque;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("Send and receive tests")
class SendAndReceiveTest extends PGMQueueTest {
    private static final Logger LOG = LoggerFactory.getLogger(SendAndReceiveTest.class);

    record TestMessage(String name, long timestamp) { }

    String QUEUE_BASIC = "my_basic_queue";
    int VISIBILITY_TIMEOUT_SEC = 30;

    @Test
    @DisplayName("Send and receive a single message")
    void sendAndReceiveMessage() {
        var queueName = QUEUE_BASIC + "_" + UUID.randomUUID().toString().replaceAll("-", "");
        var pgmq = prepareAndGetPgmq();

        LOG.info("Creating queue: {}", queueName);
        pgmq.create(queueName);

        // Send and read a message test
        var messageToSend = new TestMessage("test", Instant.now().getEpochSecond());

        pgmq.send(queueName, messageToSend);
        var message = pgmq.read(queueName)
                .as(TestMessage.class)
                .visibilityTime(VISIBILITY_TIMEOUT_SEC)
                .oneValue();

        // Check if the message is present and the message is the same as the one sent
        assertTrue(message::isPresent);
        assertEquals(message.get().getMessage(), messageToSend);

        message.ifPresent(m -> LOG.info("Message: {}, enqueuedAt: {}, readCount: {}, vt: {}",
                m.getMessage(), m.getEnqueuedAt(), m.getReadCount(), m.getVisibilityTime())
        );
    }

    @Test
    @DisplayName("Send and receive a multiple messages in batch")
    void sendAndReceiveMultipleMessages() {
        var pgmq = prepareAndGetPgmq();
        var queueName = QUEUE_BASIC + "_" + UUID.randomUUID().toString().replaceAll("-", "");
        pgmq.create(queueName);

        // Create 10 TestMessage objects with stream
        var messages = Stream.generate(() -> new TestMessage("test", Instant.now().getEpochSecond()))
                .limit(10)
                .toList();

        pgmq.sendBatch(queueName, messages);

        // Read batch of messages
        var fetchedMessages = pgmq.read(queueName)
                .as(TestMessage.class)
                .visibilityTime(VISIBILITY_TIMEOUT_SEC)
                .values(10);

        // Print fetched messages
        fetchedMessages.forEach(m ->
                LOG.info("Message: {}, enqueuedAt: {}, readCount: {}, vt: {}",
                        m.getMessage(), m.getEnqueuedAt(), m.getReadCount(), m.getVisibilityTime())
        );

        assertEquals(fetchedMessages.stream().map(Message::getMessage).toList(), messages);
    }

    @Nested
    @DisplayName("Polling tests")
    class PollingTest {

        @Test
        @DisplayName("Send and receive a multiple messages in batch with polling")
        void pollingReceiveMessageTests() {
            var pgmq = prepareAndGetPgmq();
            var queueName = QUEUE_BASIC + "_" + UUID.randomUUID().toString().replaceAll("-", "");
            pgmq.create(queueName);

            ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);
            Deque<TestMessage> localQueue = new ConcurrentLinkedDeque<>();

            // Send a message every 1 second
            var scheduledSender = executor.scheduleAtFixedRate(() -> {
                var messageToSend = new TestMessage("test", Instant.now().getEpochSecond());
                LOG.info(String.valueOf(messageToSend));
                localQueue.push(messageToSend);
                pgmq.send(queueName, messageToSend);
            }, 5, 500, TimeUnit.MILLISECONDS);

            var startTime = Instant.now();
            var endTime = startTime.plusSeconds(15);
            while (true) {
                var messages = pgmq.read(queueName)
                        .as(TestMessage.class)
                        .visibilityTime(VISIBILITY_TIMEOUT_SEC)
                        .polling()
                        .values(10);

                if (!messages.isEmpty()) {
                    LOG.info("Fetched messages");
                }
                messages.forEach(m -> {
                    LOG.info("Message: {}, enqueuedAt: {}, readCount: {}, vt: {}",
                            m.getMessage(), m.getEnqueuedAt(), m.getReadCount(), m.getVisibilityTime());

                    var polledMessage = localQueue.poll();
                    LOG.info("Polled message: {}", polledMessage);
                    assertEquals(polledMessage, m.getMessage(), "Sent message is not equal to the fetched message");
                });

                if (Instant.now().isAfter(endTime)) {
                    scheduledSender.cancel(true);
                    break;
                }
            }

            // Check if the local queue is empty, if not fetch the remaining with pgmq
            if (!localQueue.isEmpty()) {
                var messages = pgmq.read(queueName)
                        .as(TestMessage.class)
                        .visibilityTime(VISIBILITY_TIMEOUT_SEC)
                        .polling()
                        .values(localQueue.size());

                messages.forEach(m -> {
                    LOG.info("Message: {}, enqueuedAt: {}, readCount: {}, vt: {}",
                            m.getMessage(), m.getEnqueuedAt(), m.getReadCount(), m.getVisibilityTime());

                    var polledMessage = localQueue.poll();
                    LOG.info("Polled message: {}", polledMessage);
                    assertEquals(polledMessage, m.getMessage(), "Sent message is not equal to the fetched message");
                });
            }

            assertEquals(localQueue.size(), 0, "Local queue should be empty");
        }
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
