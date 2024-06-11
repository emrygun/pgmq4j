package io.tembo.pgmq;

import java.time.Duration;
import java.time.Instant;

public record Message<T> (
    Integer messageId,
    Integer readCt,
    Instant enqueuedAt,
    Duration visibilityTime,
    T message
) {}
