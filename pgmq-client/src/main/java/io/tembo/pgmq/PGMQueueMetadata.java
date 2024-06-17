package io.tembo.pgmq;

import java.time.Instant;

public record PGMQueueMetadata(
        String queueName,
        Instant createdAt,
        boolean isUnlogged,
        boolean isPartitioned
) {
}
