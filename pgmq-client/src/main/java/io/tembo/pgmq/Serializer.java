package io.tembo.pgmq;

public interface Serializer {
    String serialize(Object obj, Class<?> type);
}
