package io.tembo.pgmq.example.basic;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.tembo.pgmq.PGMQueueFactory;

import static io.tembo.pgmq.example.basic.DatabaseUtils.dataSource;
import static io.tembo.pgmq.example.basic.DatabaseUtils.postgresContainer;

public class Main {

    private static final String QUEUE_BASIC = "my_basic_queue";
    private static final Integer VISIBILITY_TIMEOUT_SEC = 30;
    private static final Integer BATCH_SIZE = 3;

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
        var basicQueue = pgmqFactory.create();
        basicQueue.create(QUEUE_BASIC);

        //language=json
        String jsonMessage = """
        { "foo": "bar" }
        """;

        basicQueue.send(QUEUE_BASIC, jsonMessage);
        var message = basicQueue.read(QUEUE_BASIC, VISIBILITY_TIMEOUT_SEC).get();
        System.out.println(message.getMessageId());

        basicQueue.delete(QUEUE_BASIC, message.getMessageId());
        System.out.println(basicQueue.read(QUEUE_BASIC, VISIBILITY_TIMEOUT_SEC));

        postgresContainer.stop();
    }
}