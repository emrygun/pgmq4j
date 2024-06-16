package io.tembo.pgmq;

import java.time.Instant;

public record Message (
    Integer messageId,
    Integer readCount,
    Instant enqueuedAt,
    Instant visibilityTime,
    String message
) {}
