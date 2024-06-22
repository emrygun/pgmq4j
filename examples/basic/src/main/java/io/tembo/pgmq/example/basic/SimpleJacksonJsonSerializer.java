package io.tembo.pgmq.example.basic;

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
    public <T> T fromJson(String json, Class<T> clazz) {
        if (clazz == String.class) {
            return (T) json;
        }
        try {
            return mapper.readValue(json, clazz);
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
