package io.tembo.pgmq;

@FunctionalInterface
public interface MessageListener {

    /**
     * Callback for processing received objects through PGMQ.
     *
     * @param message message must not be {@literal null}.
     * @param pattern pattern matching the channel (if specified) - can be {@literal null}.
     */
    void onMessage(Message message, /*@Nullable*/ String pattern);
}
