package dev.emreuygun.pgmq;

import java.time.Instant;

/**
 * Represents a message in a queue.
 */
public abstract class AbstractMessage<T> {
    MessageId messageId;

    Integer readCount;

    Instant enqueuedAt;

    Instant visibilityTime;

    T message;

    public AbstractMessage(MessageId messageId, Integer readCount, Instant enqueuedAt, Instant visibilityTime, T message) {
        this.messageId = messageId;
        this.readCount = readCount;
        this.enqueuedAt = enqueuedAt;
        this.visibilityTime = visibilityTime;
        this.message = message;
    }

    public MessageId getMessageId() {
        return messageId;
    }

    public void setMessageId(MessageId messageId) {
        this.messageId = messageId;
    }

    public Integer getReadCount() {
        return readCount;
    }

    public void setReadCount(Integer readCount) {
        this.readCount = readCount;
    }

    public Instant getEnqueuedAt() {
        return enqueuedAt;
    }

    public void setEnqueuedAt(Instant enqueuedAt) {
        this.enqueuedAt = enqueuedAt;
    }

    public Instant getVisibilityTime() {
        return visibilityTime;
    }

    public void setVisibilityTime(Instant visibilityTime) {
        this.visibilityTime = visibilityTime;
    }

    public T getMessage() {
        return message;
    }

    public void setMessage(T message) {
        this.message = message;
    }
}
