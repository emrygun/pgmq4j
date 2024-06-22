package dev.emreuygun.pgmq;

import java.time.Instant;

/**
 * Represents a message in a queue.
 * @param <T> Message type
 */
public class Message<T> extends AbstractMessage<T> {
    public Message(MessageId messageId, Integer readCount, Instant enqueuedAt, Instant visibilityTime, T message) {
        super(messageId, readCount, enqueuedAt, visibilityTime, message);
    }
}
