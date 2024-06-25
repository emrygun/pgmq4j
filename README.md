# pgmq4j Java Client Library

This library provides a Java interface for interacting with [PGMQ](https://github.com/tembo-io/pgmq), a message queue system built on top of PostgreSQL. 

## Example Usage

Here's a basic example of sending and reading a message from a PGMQ queue:
```java
// Create a queue
var pgmqFactory = new PGMQueueFactory(jsonSerializer, connectionProvider);
var pgmq = pgmqFactory.create();
pgmq.create("my_queue");

// Send and read a message test
TestMessage testMessage = new TestMessage("test", Instant.now().getEpochSecond());
pgmq.send("my_queue", testMessage);

var message = pgmq.read(QUEUE_BASIC)
    .as(TestMessage.class)
    .visibilityTime(VISIBILITY_TIMEOUT_SEC)
    .oneValue();

assert message.isPresent();

// Send multiple messages and read values
var messages = Stream.generate(() -> new TestMessage("test", Instant.now().getEpochSecond()))
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

## Contributing

We welcome contributions to this project. Please see our [CONTRIBUTING.md](CONTRIBUTING.md) for more details.

## License

This project is licensed under the terms of the MIT license. See the [LICENSE](LICENSE) file for details.