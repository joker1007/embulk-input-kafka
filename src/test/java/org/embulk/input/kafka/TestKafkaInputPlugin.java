package org.embulk.input.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser.Feature;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
      .registerModules(new Jdk8Module(), new JavaTimeModule())
      .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
      .configure(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS, false);

  private List<String> topicNames = ImmutableList.of("json-simple-topic", "json-complex-topic");

  @Before
  public void setUp()
  {
    kafkaTestUtils = sharedKafkaTestResource.getKafkaTestUtils();
    topicNames.forEach(topic -> {
      kafkaTestUtils.createTopic(topic, 8, (short) 1);
    });
  }

  @After
  public void tearDown()
  {
    kafkaTestUtils.getAdminClient().deleteTopics(topicNames);
  }

  @Test
  public void testSimpleJson() throws IOException
  {
    IntStream.rangeClosed(0, 7).forEach(i -> {
      Map<byte[], byte[]> records = new HashMap<>();
      IntStream.rangeClosed(0, 2).forEach(j -> {
        String recordId = "ID-" + i + "-" + j;
        SimpleRecord simpleRecord = new SimpleRecord(recordId, j, "varchar_" + j);
        try {
          String value = objectMapper.writeValueAsString(simpleRecord);
          records.put(recordId.getBytes(), value.getBytes());
        } catch (JsonProcessingException e) {
          throw new RuntimeException(e);
        }
      });
      kafkaTestUtils.produceRecords(records, "json-simple-topic", i);
    });
    ConfigSource configSource = embulk.loadYamlResource("config_simple.yml");
    configSource.set("brokers", ImmutableList.of(sharedKafkaTestResource.getKafkaBrokers().getBrokerById(1).getConnectString()));
    Path outputDir = Files.createTempDirectory("embulk-input-kafka-test-simple-json");
    Path outputPath = outputDir.resolve("out.csv");
    embulk.runInput(configSource, outputPath);
    CsvMapper csvMapper = new CsvMapper();
    ObjectReader objectReader = csvMapper.readerWithTypedSchemaFor(SimpleRecord.class);
    MappingIterator<SimpleRecord> it = objectReader
        .readValues(outputPath.toFile());

    List<SimpleRecord> outputs = new ArrayList<>();
    it.forEachRemaining(outputs::add);

    assertEquals(24, outputs.size());
    SimpleRecord simpleRecord = outputs.stream().filter(r -> r.getId().equals("ID-0-1"))
        .findFirst().get();
    assertEquals(1, simpleRecord.getIntItem().intValue());
  }

  @Test
  public void testComplexJson() throws IOException
  {
    IntStream.rangeClosed(0, 7).forEach(i -> {
      Map<byte[], byte[]> records = new HashMap<>();
      IntStream.rangeClosed(0, 2).forEach(j -> {
        String recordId = "ID-" + i + "-" + j;
        ComplexRecord complexRecord = new ComplexRecord(
            recordId,
            j,
            "varchar_" + j,
            Instant.ofEpochMilli(1597510800000L), // 2020-08-15 17:00:00 +00:00
            ImmutableList.of("hoge" + j, "fuga" + j),
            ImmutableMap.of("key", ImmutableMap.of("inner-1", "value" + j, "inner-2", "value" + j)));
        try {
          String value = objectMapper.writeValueAsString(complexRecord);
          records.put(recordId.getBytes(), value.getBytes());
        } catch (JsonProcessingException e) {
          throw new RuntimeException(e);
        }
      });
      kafkaTestUtils.produceRecords(records, "json-complex-topic", i);
    });
    ConfigSource configSource = embulk.loadYamlResource("config_complex.yml");
    configSource.set("brokers", ImmutableList.of(sharedKafkaTestResource.getKafkaBrokers().getBrokerById(1).getConnectString()));
    Path outputDir = Files.createTempDirectory("embulk-input-kafka-test-complex-json");
    Path outputPath = outputDir.resolve("out.csv");
    embulk.runInput(configSource, outputPath);

    CsvMapper csvMapper = new CsvMapper();
    csvMapper.enable(Feature.WRAP_AS_ARRAY);
    MappingIterator<String[]> it = csvMapper.readerFor(String[].class)
        .readValues(outputPath.toFile());

    List<String[]> outputs = new ArrayList<>();
    it.forEachRemaining(outputs::add);

    assertEquals(24, outputs.size());

    String[] row = outputs.stream().filter(r -> r[0].equals("ID-0-1")).findFirst().get();
    assertEquals("1", row[1]);
    assertEquals("varchar_1", row[2]);
    assertEquals("2020-08-15 17:00:00.000000 +0000", row[3]);

    List<String> arrayData = objectMapper.readValue(row[4],
        objectMapper.getTypeFactory().constructCollectionType(List.class, String.class));
    assertEquals("hoge1", arrayData.get(0));
    assertEquals("fuga1", arrayData.get(1));

    JsonNode objectData = objectMapper.readTree(row[5]);
    assertTrue(objectData.has("key"));
    assertTrue(objectData.get("key").has("inner-1"));
    assertEquals("value1", objectData.get("key").get("inner-1").asText());
    assertTrue(objectData.get("key").has("inner-2"));
    assertEquals("value1", objectData.get("key").get("inner-2").asText());
  }
}
