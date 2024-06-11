package io.tembo.pgmq;

import java.time.Instant;

public record QueueMetrics(
        String queueName,
        Integer queueLength,
        Integer newestMessageAgeSec,
        Integer oldestMessageAgeSec,
        Integer totalMessages,
        Instant scrapeTime
) {
}
