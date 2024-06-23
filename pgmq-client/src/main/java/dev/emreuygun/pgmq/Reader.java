package dev.emreuygun.pgmq;


import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

public class Reader<T> {
    private static final Duration DEFAULT_POLLING_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration DEFAULT_POLLING_INTERVAL = Duration.ofSeconds(5);
    private static final PollingConfig DEFAULT_POLLING_CONFIG = new PollingConfig(DEFAULT_POLLING_TIMEOUT, DEFAULT_POLLING_INTERVAL);

    private final PGMQueue queue;
    private final PGMQClient client;

    private final String queueName;
    private int visibilityTime;
    @SuppressWarnings("rawtypes")
    private Class clazz;

    private PollingConfig pollingConfig = null;

    Reader(PGMQueue queue, PGMQClient client, String queueName) {
        this.queue = queue;
        this.client = client;
        this.queueName = queueName;

        this.visibilityTime = 30;
        this.clazz = String.class;
    }

    protected Reader(Reader<?> reader) {
        this.queue = reader.queue;
        this.queueName = reader.queueName;
        this.clazz = reader.clazz;
        this.visibilityTime = reader.visibilityTime;
        this.client = reader.client;
        this.pollingConfig = reader.pollingConfig;
    }

    @SuppressWarnings("unchecked")
    public <V> Reader<V> as(Class<V> clazz) {
        this.clazz = clazz;
        return (Reader<V>) this;
    }

    public Reader<T> visibilityTime(int visibilityTime) {
        this.visibilityTime = visibilityTime;
        return this;
    }

    public Reader<T> polling(Duration pollingTimeout, Duration pollingInterval) {
        pollingTimeout = Optional.ofNullable(pollingTimeout).orElse(Duration.ofSeconds(30));
        this.pollingConfig = new PollingConfig(pollingTimeout, pollingInterval);
        return this;
    }

    public Reader<T> polling() {
        this.pollingConfig = DEFAULT_POLLING_CONFIG;
        return this;
    }

    /**
     * Reads a single
     * @return an optional value
     */
    public Optional<Message<T>> oneValue() {
        var byteArrayMessage = pollingConfig != null ?
                withPoll(() -> client.read(queueName, visibilityTime)) :
                client.read(queueName, visibilityTime);

        return byteArrayMessage.map(this::toMessage);
    }

    /**
     * Reads batch of values
     * @return a list of values
     */
    public List<Message<T>> values(int limit) {
        var values = pollingConfig != null ?
                withPoll(() -> client.readBatch(queueName, visibilityTime, limit)) :
                client.readBatch(queueName, visibilityTime, limit);

        return values.stream().map(this::toMessage).toList();
    }


    private record PollingConfig(Duration pollingTimeout, Duration pollingInterval) {
    }

    /**
     * //TODO: Better implementation needed
     *
     * Polls for a value using the configured polling interval and timeout.
     * @param valueSupplier the supplier to poll for a value
     * @return the value if found, otherwise null
     * @param <V> the type of the value
     */
    private <V> V withPoll(Supplier<V> valueSupplier) {
        if (pollingConfig == null) {
            return valueSupplier.get();
        }

        var pollingTimeout = pollingConfig.pollingTimeout;
        var pollingInterval = pollingConfig.pollingInterval;

        var startTime = System.currentTimeMillis();
        var endTime = startTime + pollingTimeout.toMillis();

        V value = valueSupplier.get();
        while (value == null && System.currentTimeMillis() < endTime) {
            try {
                Thread.sleep(pollingInterval.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
            value = valueSupplier.get();
        }

        return value;
    }

    @SuppressWarnings("unchecked")
    private Message<T> toMessage(ByteArrayMessage byteArrayMessage) {
        T convertedRecord = (T) queue.getJsonSerializer().fromJson(new String(byteArrayMessage.getMessage(), StandardCharsets.UTF_8), clazz);
        return new Message<T>(byteArrayMessage.getMessageId(),
                byteArrayMessage.getReadCount(),
                byteArrayMessage.getEnqueuedAt(),
                byteArrayMessage.getVisibilityTime(),
                convertedRecord);
    }
}
