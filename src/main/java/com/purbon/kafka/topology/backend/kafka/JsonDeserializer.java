package com.purbon.kafka.topology.backend.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.purbon.kafka.topology.backend.BackendState;
import java.io.IOException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class JsonDeserializer<T> implements Deserializer<BackendState> {

  private final ObjectMapper objectMapper = new ObjectMapper();

  public JsonDeserializer() {}

  public JsonDeserializer(Class<T> tClass) {}

  @Override
  public BackendState deserialize(String s, byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    BackendState data;
    try {
      data = objectMapper.readValue(bytes, BackendState.class);
    } catch (IOException e) {
      throw new SerializationException(e);
    }
    return data;
  }
}
