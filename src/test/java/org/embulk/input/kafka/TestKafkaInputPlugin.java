package org.embulk.input.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
import com.salesforce.kafka.test.KafkaTestUtils;
import com.salesforce.kafka.test.junit4.SharedKafkaTestResource;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.BytesSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.embulk.config.ConfigSource;
import org.embulk.formatter.csv.CsvFormatterPlugin;
import org.embulk.output.file.LocalFileOutputPlugin;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.FormatterPlugin;
import org.embulk.spi.InputPlugin;
import org.embulk.test.TestingEmbulk;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class TestKafkaInputPlugin
{

  @Rule
  public final SharedKafkaTestResource sharedKafkaTestResource = new SharedKafkaTestResource()
      .withBrokers(1);

  @Rule
  public TestingEmbulk embulk = TestingEmbulk.builder()
      .registerPlugin(InputPlugin.class, "kafka", KafkaInputPlugin.class)
      .registerPlugin(FileOutputPlugin.class, "file", LocalFileOutputPlugin.class)
      .registerPlugin(FormatterPlugin.class, "csv", CsvFormatterPlugin.class)
      .build();

  private KafkaTestUtils kafkaTestUtils;
  private final static ObjectMapper objectMapper = new ObjectMapper()
      .registerModules(new Jdk8Module(), new JavaTimeModule())
      .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
      .configure(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS, false);

  private final List<String> topicNames = Arrays.asList("json-simple-topic", "json-complex-topic", "avro-simple-topic", "avro-complex-topic");

  @Before
  public void setUp() {
    kafkaTestUtils = sharedKafkaTestResource.getKafkaTestUtils();
    topicNames.forEach(topic -> {
      kafkaTestUtils.createTopic(topic, 48, (short) 1);
    });
  }

  @After
  public void tearDown() {
    kafkaTestUtils.getAdminClient().deleteTopics(topicNames);
  }

  private List<String> brokersConfig()
  {
    return Collections.singletonList(
        sharedKafkaTestResource.getKafkaBrokers().getBrokerById(1).getConnectString());
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
    configSource.set("brokers", brokersConfig());
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
        ComplexRecord complexRecord;
        if (j == 2) {
          Map<String, String> innerMap = new HashMap<>();
          innerMap.put("inner-1", null);
          complexRecord = new ComplexRecord(
              recordId,
              null,
              null,
              null,
              null,
              Collections.singletonMap("key", innerMap));
        }
        else {
          Map<String, String> innerMap = Stream.of(new String[][]{
              {"inner-1", "value" + j},
              {"inner-2", "value" + j}
          }).collect(Collectors.toMap(data -> data[0], data -> data[1]));
          complexRecord = new ComplexRecord(
              recordId,
              j,
              "varchar_" + j,
              Instant.ofEpochMilli(1597510800000L), // 2020-08-15 17:00:00 +00:00
              Arrays.asList("hoge" + j, "fuga" + j),
              Collections.singletonMap("key", innerMap));
        }
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
    configSource.set("brokers", brokersConfig());
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

    assertEquals("ID-0-1", row[6]);
    assertEquals("0", row[7]);
  }

  @Test
  public void testSimpleJsonWithTimestampSeek() throws IOException
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
    configSource.set("brokers", brokersConfig());
    configSource.set("seek_mode", "timestamp");
    long now = Instant.now().toEpochMilli();
    configSource.set("timestamp_for_seeking", now);

    IntStream.rangeClosed(0, 7).forEach(i -> {
      Map<byte[], byte[]> records = new HashMap<>();
      IntStream.rangeClosed(0, 0).forEach(j -> {
        String recordId = "ID-AFTER-" + i + "-" + j;
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

    Path outputDir = Files.createTempDirectory("embulk-input-kafka-test-simple-json");
    Path outputPath = outputDir.resolve("out.csv");
    embulk.runInput(configSource, outputPath);
    CsvMapper csvMapper = new CsvMapper();
    ObjectReader objectReader = csvMapper.readerWithTypedSchemaFor(SimpleRecord.class);
    MappingIterator<SimpleRecord> it = objectReader
        .readValues(outputPath.toFile());

    List<SimpleRecord> outputs = new ArrayList<>();
    it.forEachRemaining(outputs::add);

    assertEquals(8, outputs.size());
    SimpleRecord simpleRecord = outputs.stream().filter(r -> r.getId().equals("ID-AFTER-0-0"))
        .findFirst().get();
    assertEquals(0, simpleRecord.getIntItem().intValue());
  }

  @Test
  public void testSimpleAvro() throws IOException
  {
    IntStream.rangeClosed(0, 7).forEach(i -> {
      List<ProducerRecord<Bytes, Object>> records = new ArrayList<>();
      IntStream.rangeClosed(0, 2).forEach(j -> {
        String recordId = "ID-" + i + "-" + j;
        SimpleRecordAvro simpleRecord = SimpleRecordAvro.newBuilder()
            .setId(recordId)
            .setIntItem(j)
            .setVarcharItem("varchar_" + j)
            .build();
        Bytes bytes = Bytes.wrap(recordId.getBytes());
        records.add(new ProducerRecord<>("avro-simple-topic", bytes, simpleRecord));
      });
      Properties producerConfigs = new Properties();
      producerConfigs.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://" + KafkaInputPlugin.MOCK_SCHEMA_REGISTRY_SCOPE);
      KafkaProducer<Bytes, Object> kafkaProducer = kafkaTestUtils
          .getKafkaProducer(BytesSerializer.class, KafkaAvroSerializer.class, producerConfigs);
      records.forEach(kafkaProducer::send);
      kafkaProducer.close();
    });
    ConfigSource configSource = embulk.loadYamlResource("config_simple_avro.yml");
    configSource.set("brokers", brokersConfig());
    Path outputDir = Files.createTempDirectory("embulk-input-kafka-test-simple-avro");
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
  public void testComplexAvro() throws IOException
  {
    IntStream.rangeClosed(0, 7).forEach(i -> {
      List<ProducerRecord<Bytes, Object>> records = new ArrayList<>();
      IntStream.rangeClosed(0, 2).forEach(j -> {
        String recordId = "ID-" + i + "-" + j;
        ComplexRecordAvro complexRecord;
        if (j == 2) {
          Map<String, Long> map = Stream.of(new Object[][] {
              {"key1", 1L},
              {"key2", 2L}
          }).collect(Collectors.toMap(data -> (String) data[0], data -> (Long) data[1]));
          complexRecord = ComplexRecordAvro.newBuilder()
              .setId(recordId)
              .setIntItem(j)
              .setVarcharItem(null)
              .setTime(1597510800000L) // 2020-08-15 17:00:00 +00:00
              .setArray(null)
              .setData(InnerData.newBuilder()
                  .setAaa(null)
                  .setHoge("hogehoge" + j)
                  .setInnerArray(Arrays.asList(4L, 5L))
                  .setInnerMap(map)
                  .build())
              .build();
        }
        else {
          Map<String, Long> map = Stream.of(new Object[][] {
              {"key1", 1L},
              {"key2", 2L}
          }).collect(Collectors.toMap(data -> (String) data[0], data -> (Long) data[1]));
          complexRecord = ComplexRecordAvro.newBuilder()
              .setId(recordId)
              .setIntItem(j)
              .setVarcharItem("varchar_" + j)
              .setTime(1597510800000L) // 2020-08-15 17:00:00 +00:00
              .setArray(Arrays.asList(1L, 2L, 3L))
              .setData(InnerData.newBuilder()
                  .setAaa("aaa" + j)
                  .setHoge("hogehoge" + j)
                  .setInnerArray(Arrays.asList(4L, 5L))
                  .setInnerMap(map)
                  .build())
              .build();
        }
        Bytes bytes = Bytes.wrap(recordId.getBytes());
        records.add(new ProducerRecord<>("avro-complex-topic", bytes, complexRecord));
      });
      Properties producerConfigs = new Properties();
      producerConfigs.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://" + KafkaInputPlugin.MOCK_SCHEMA_REGISTRY_SCOPE);
      KafkaProducer<Bytes, Object> kafkaProducer = kafkaTestUtils
          .getKafkaProducer(BytesSerializer.class, KafkaAvroSerializer.class, producerConfigs);
      records.forEach(kafkaProducer::send);
      kafkaProducer.close();
    });
    ConfigSource configSource = embulk.loadYamlResource("config_complex_avro.yml");
    configSource.set("brokers", brokersConfig());
    Path outputDir = Files.createTempDirectory("embulk-input-kafka-test-complex-avro");
    Path outputPath = outputDir.resolve("out.csv");
    embulk.runInput(configSource, outputPath);

    CsvMapper csvMapper = new CsvMapper();
    csvMapper.enable(Feature.WRAP_AS_ARRAY);
    MappingIterator<String[]> it = csvMapper.readerFor(String[].class)
        .readValues(outputPath.toFile());

    List<String[]> outputs = new ArrayList<>();
    it.forEachRemaining(outputs::add);

    assertEquals(24, outputs.size());
  }
}
