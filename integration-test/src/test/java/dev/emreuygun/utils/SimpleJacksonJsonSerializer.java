package dev.emreuygun.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.emreuygun.pgmq.JsonSerializer;

import java.io.IOException;

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
