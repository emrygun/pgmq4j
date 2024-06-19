package io.tembo.pgmq;

public interface JsonSerializer {
    <T> T fromJson(java.lang.String json, java.lang.Class<T> classOfT);

    String toJson(Object src);
}
