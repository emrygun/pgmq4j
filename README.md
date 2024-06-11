# pgmq4j the "pgmq Java Client Library"

This library provides a Java interface for interacting with [PGMQ](https://github.com/tembo-io/pgmq), a message queue system built on top of PostgreSQL. 

pgmq-client accomplishes most of primitive features of pgmq, including:
- Creating or deleting queues
- Sending messages
- Reading and batch reading in normal or long polling mode
- Archiving messages
- Deleting and popping messages

... and so on


## Example Usages

Here are some examples of how to use the library.

### Create a queue:
```java
PGMQueueFactory pgmqFactory = new PGMQueueFactory(jsonSerializer, connectionProvider);
PGMQueue pgmq = pgmqFactory.create();
pgmq.create("my_queue");
```

### Send and read a message:
```java
// Send and read a message test
TestMessage testMessage = new TestMessage("test", Instant.now().getEpochSecond());
pgmq.send("my_queue", testMessage);

Optional<TestMessage> message = pgmq.read("my_queue")
    .as(TestMessage.class)
    .visibilityTime(30)
    .oneValue();
```

### Send and read multiple messages:
```java
// Send multiple messages and read values
List<TestMessage> messages = Stream.generate(() -> new TestMessage("test", Instant.now().getEpochSecond()))
    .limit(10)
    .toList();
pgmq.sendBatch(queueName, messages);

pgmq.read("my_queue")
    .as(TestMessage.class)
    .visibilityTime(30)
    .polling()
    .values(10)
    .forEach(m ->
    LOG.info("Message: {}, enqueuedAt: {}, readCount: {}, vt: {}",
            m.getMessage(),
            m.getEnqueuedAt(),
            m.getReadCount(),
            m.getVisibilityTime())
    );
```

## Project Stage
Project is still on development stage, and not ready for production use.
Still requires more tests, improvements and adaptations for the Java world.


## Contributing

Contributions are what make the open-source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

Feel free to open issues if you find any, I appreciate any help in making the project better.

## License

This project is licensed under the terms of the MIT license. See the [LICENSE](LICENSE) file for details.