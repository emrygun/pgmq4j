package dev.emreuygun;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.emreuygun.infra.PGMQueueTest;
import dev.emreuygun.pgmq.JsonSerializer;
import dev.emreuygun.pgmq.PGMQError;
import dev.emreuygun.pgmq.PGMQueue;
import dev.emreuygun.pgmq.PGMQueueFactory;
import dev.emreuygun.utils.DatabaseUtils;
import dev.emreuygun.utils.PgConnectionProvider;
import dev.emreuygun.utils.SimpleJacksonJsonSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


@DisplayName("Create queue and destroy tests")
class CreateAndDestroyTest extends PGMQueueTest {
    private static final Logger LOG = LoggerFactory.getLogger(CreateAndDestroyTest.class);

    @Test
    @DisplayName("Create a queue and destroy it")
    void createQueueAndDestroy() {
        var pgmq = prepareAndGetPgmq();
        var queueName = "my_queue_" + System.currentTimeMillis();

        pgmq.create(queueName);
        pgmq.destroy(queueName);

        Assertions.assertDoesNotThrow(() -> pgmq.create(queueName));
        Assertions.assertDoesNotThrow(() -> pgmq.destroy(queueName));
    }

    @Test
    @DisplayName("Create a queue, list the queues and check if present then destroy it")
    void createQueueGetListAndDestroy() {
        var pgmq = prepareAndGetPgmq();
        var queueName = "my_queue_" + System.currentTimeMillis();

        pgmq.create(queueName);

        var queues = pgmq.listQueues();
        LOG.info(queues.toString());

        var queueMetrics = queues.stream().filter(q -> q.queueName().equals(queueName)).findFirst();

        assertTrue(queueMetrics.isPresent());
        assertEquals(queueMetrics.get().queueName(), queueName);
        assertTrue(queueMetrics.get().isUnlogged());
        assertFalse(queueMetrics.get().isPartitioned());

        pgmq.destroy(queueName);
    }

    @Test
    @DisplayName("Try to use a queue that does not exist")
    void useQueueThatDoesNotExist() {
        var pgmq = prepareAndGetPgmq();
        var queueName = "my_queue_" + System.currentTimeMillis();

        Assertions.assertThrows(PGMQError.class, () -> pgmq.send(queueName, "test"));
    }

    @Test
    @DisplayName("Create a queue already exists")
    void createQueueAlreadyExists() {
        var pgmq = prepareAndGetPgmq();
        var queueName = "my_queue_" + System.currentTimeMillis();

        pgmq.create(queueName);
        pgmq.destroy(queueName);

        Assertions.assertDoesNotThrow(() -> pgmq.create(queueName));
        Assertions.assertDoesNotThrow(() -> pgmq.create(queueName));
        Assertions.assertDoesNotThrow(() -> pgmq.create(queueName));
    }

    @Test
    @DisplayName("Delete a queue that does not exist")
    void deleteQueueThatDoesNotExist() {
        var pgmq = prepareAndGetPgmq();
        var queueName = "my_queue_" + System.currentTimeMillis();

        Assertions.assertDoesNotThrow(() -> pgmq.destroy(queueName));
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
