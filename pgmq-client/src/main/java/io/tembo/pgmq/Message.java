package io.tembo.pgmq;

import java.time.Instant;

public interface Message {
    Integer getMessageId();

    Integer getReadCount();

    Instant getEnqueuedAt();

    Instant getVisibilityTime();

    byte[] getMessage();
}
