package io.tembo.pgmq;

import java.time.Instant;

/**
 * Default implementation of {@link AbstractMessage}.
 */
public class ByteArrayMessage extends AbstractMessage<byte[]> {
    /**
     * @param messageId MessageId
     * @param readCount Read count
     * @param enqueuedAt Enqueued at
     * @param visibilityTime Visibility time
     * @param message AbstractMessage bytes or empty array if no message is present
     */
    public ByteArrayMessage(MessageId messageId, Integer readCount, Instant enqueuedAt, Instant visibilityTime, byte[] message) {
        super(messageId, readCount, enqueuedAt, visibilityTime, message);
    }
}
