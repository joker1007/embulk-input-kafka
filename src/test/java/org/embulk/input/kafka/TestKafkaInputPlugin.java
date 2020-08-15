package org.embulk.input.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.collect.ImmutableList;
import com.salesforce.kafka.test.KafkaTestUtils;
import com.salesforce.kafka.test.junit4.SharedKafkaTestResource;
import org.embulk.config.ConfigSource;
import org.embulk.spi.InputPlugin;
import org.embulk.test.TestingEmbulk;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

public class TestKafkaInputPlugin
{

  @ClassRule
  public static final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource()
      .withBrokers(3);

  @Rule
  public TestingEmbulk embulk = TestingEmbulk.builder()
      .registerPlugin(InputPlugin.class, "kafka", KafkaInputPlugin.class)
      .build();

  private KafkaTestUtils kafkaTestUtils;
  private final static ObjectMapper objectMapper = new ObjectMapper()
      .registerModules(new Jdk8Module(), new JavaTimeModule());

  @Before
  public void setUp()
  {
    kafkaTestUtils = sharedKafkaTestResource.getKafkaTestUtils();
    kafkaTestUtils.createTopic("json-simple-topic", 8, (short) 1);
  }

  @After
  public void tearDown()
  {
    kafkaTestUtils.getAdminClient().deleteTopics(ImmutableList.of(
        "json-simple-topic"
    ));
  }

  @Test
  public void testSimpleJson() throws IOException
  {
    IntStream.rangeClosed(0, 7).forEach(i -> {
      Map<byte[], byte[]> records = new HashMap<>();
      IntStream.rangeClosed(0, 2).forEach(j -> {
        ObjectNode objectNode = objectMapper.createObjectNode();
        String recordId = "A-" + i + "-" + j;
        objectNode.put("id", recordId);
        objectNode.put("int_item", j);
        objectNode.put("varchar_item", "varchar_" + j);
        try {
          String value = objectMapper.writeValueAsString(objectNode);
          records.put(recordId.getBytes(), value.getBytes());
        } catch (JsonProcessingException e) {
          throw new RuntimeException(e);
        }
      });
      kafkaTestUtils.produceRecords(records, "json-simple-topic", i);
    });
    ConfigSource configSource = embulk.loadYamlResource("config_simple.yml");
    configSource.set("brokers", ImmutableList.of(sharedKafkaTestResource.getKafkaBrokers().getBrokerById(1).getConnectString()));
    Path outputDir = Files.createTempDirectory("embulk-input-kafka");
    Path outputPath = outputDir.resolve("out-simple-json.csv");
    embulk.runInput(configSource, outputPath);
  }
}
