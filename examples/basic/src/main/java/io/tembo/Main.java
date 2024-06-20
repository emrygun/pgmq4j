package io.tembo;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.tembo.pgmq.PGMQueueFactory;

import static io.tembo.DatabaseUtils.dataSource;
import static io.tembo.DatabaseUtils.postgresContainer;

public class Main {
    public static void main(String[] args) {
        postgresContainer.start();
        dataSource.setURL(postgresContainer.getJdbcUrl());

        String QUEUE_BASIC = "my_basic_queue";
        Integer VISIBILITY_TIMEOUT_SEC = 30;
        Integer BATCH_SIZE = 3;

        var objectMapper = new ObjectMapper();
        var jsonSerializer = new SimpleJacksonJsonSerializer(objectMapper);

        // Initialize pgmq instance
        var pgmqFactory = new PGMQueueFactory(jsonSerializer, dataSource);
        var basicQueue = pgmqFactory.create(QUEUE_BASIC);

        //language=json
        String jsonMessage = """
        { "foo": "bar" }
        """;

        basicQueue.send(jsonMessage);
        var message = basicQueue.read(VISIBILITY_TIMEOUT_SEC).get();
        System.out.println(message.getMessageId());

        basicQueue.delete(message.getMessageId());
        System.out.println(basicQueue.read(VISIBILITY_TIMEOUT_SEC));

        postgresContainer.stop();
    }
}