package dev.emreuygun.pgmq;

/**
 * Interface for JSON serialization.
 */
public interface JsonSerializer {
    <T> T fromJson(String json, Class<T> clazz);

    String toJson(Object src);
}
