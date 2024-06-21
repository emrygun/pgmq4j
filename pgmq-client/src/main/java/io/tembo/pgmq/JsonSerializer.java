package io.tembo.pgmq;

/**
 * Interface for JSON serialization.
 */
public interface JsonSerializer {
    <T> T fromJson(String json, Class<T> classOfT);

    String toJson(Object src);
}
