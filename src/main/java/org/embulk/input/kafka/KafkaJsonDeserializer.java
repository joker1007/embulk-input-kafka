package org.embulk.input.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class KafkaJsonDeserializer implements Deserializer<ObjectNode>
{
  private static Logger logger = LoggerFactory.getLogger(KafkaJsonDeserializer.class);
  private static ObjectMapper mapper = new ObjectMapper()
      .registerModules(new Jdk8Module(), new JavaTimeModule());

  @Override
  public ObjectNode deserialize(String topic, byte[] data)
  {
    try {
      JsonNode jsonNode = mapper.readTree(data);
      if (jsonNode.isObject()) {
        return (ObjectNode) jsonNode;
      }
      else {
        logger.warn("Ignore current record that is not an object: {}", data);
        return null;
      }
    }
    catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }
}
