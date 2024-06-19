package io.tembo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.tembo.pgmq.JsonSerializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class SimpleJacksonJsonSerializer implements JsonSerializer {
    private final ObjectMapper mapper;

    public SimpleJacksonJsonSerializer(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public <T> T fromJson(String json, Class<T> classOfT) {
        try {
            return mapper.readValue(json.getBytes(StandardCharsets.UTF_8), classOfT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toJson(Object src) {
        try {
            return mapper.writeValueAsString(src);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}