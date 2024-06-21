package io.tembo.pgmq;

import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;

/**
 * Default implementation of {@link Message}.
 */
public class DefaultMessage implements Message {
    private final MessageId messageId;
    private final Integer readCount;
    private final Instant enqueuedAt;
    private final Instant visibilityTime;
    private final byte[] message;

    /**
     * @param messageId MessageId
     * @param readCount Read count
     * @param enqueuedAt Enqueued at
     * @param visibilityTime Visibility time
     * @param message Message bytes or empty array if no message is present
     */
    DefaultMessage(MessageId messageId, Integer readCount, Instant enqueuedAt, Instant visibilityTime, byte[] message) {
        this.messageId = messageId;
        this.readCount = readCount;
        this.enqueuedAt = enqueuedAt;
        this.visibilityTime = visibilityTime;
        this.message = message;
    }

    @Override
    public MessageId getMessageId() {
        return messageId;
    }

    @Override
    public Integer getReadCount() {
        return readCount;
    }

    @Override
    public Instant getEnqueuedAt() {
        return enqueuedAt;
    }

    @Override
    public Instant getVisibilityTime() {
        return visibilityTime;
    }

    @Override
    public byte[] getMessage() {
        return message.length == 0 ? message : message.clone();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DefaultMessage that = (DefaultMessage) o;
        return Objects.equals(messageId, that.messageId) &&
                Objects.equals(readCount, that.readCount) &&
                Objects.equals(enqueuedAt, that.enqueuedAt) &&
                Objects.equals(visibilityTime, that.visibilityTime) &&
                Arrays.equals(message, that.message);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(messageId, readCount, enqueuedAt, visibilityTime);
        result = 31 * result + Arrays.hashCode(message);
        return result;
    }
}
