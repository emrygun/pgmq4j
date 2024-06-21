package io.tembo.pgmq;

import java.time.Instant;

/**
 * Represents a message in a queue.
 */
public interface Message {
    MessageId getMessageId();

    Integer getReadCount();

    Instant getEnqueuedAt();

    Instant getVisibilityTime();

    byte[] getMessage();
}
