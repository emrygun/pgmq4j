package io.tembo.pgmq;

public interface JsonSerializer {
    <T> T fromJson(String json, Class<T> classOfT);

    String toJson(Object src);
}
