package org.embulk.input.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Bytes;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.Exec;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.Schema;
import org.embulk.spi.SchemaConfig;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class KafkaInputPlugin
        implements InputPlugin
{
    public enum RecordSerializeFormat
    {
        JSON,
        AVRO_WITH_SCHEMA_REGISTRY;

        @JsonValue
        public String toString()
        {
            return name().toLowerCase(Locale.ENGLISH);
        }

        @JsonCreator
        public static RecordSerializeFormat ofString(String name)
        {
            switch (name.toLowerCase(Locale.ENGLISH)) {
                case "json":
                    return JSON;
                case "avro_with_schema_registry":
                    return AVRO_WITH_SCHEMA_REGISTRY;
                default:
            }

            throw new ConfigException(String.format("Unknown serialize format '%s'. Supported modes are json, avro_with_schema_registry", name));
        }
    }

    public interface PluginTask
            extends Task
    {
        @Config("brokers")
        public List<String> getBrokers();

        @Config("topics")
        public List<String> getTopics();

        @Config("schema_registry_url")
        @ConfigDefault("null")
        public java.util.Optional<String> getSchemaRegistryUrl();

        @Config("serialize_format")
        public RecordSerializeFormat getRecordSerializeFormat();

        @Config("avsc_file")
        @ConfigDefault("null")
        public java.util.Optional<File> getAvscFile();

        @Config("avsc")
        @ConfigDefault("null")
        public java.util.Optional<ObjectNode> getAvsc();

        @Config("key_column_name")
        @ConfigDefault("null")
        public java.util.Optional<String> getKeyColumnName();

        @Config("partition_column_name")
        @ConfigDefault("null")
        public java.util.Optional<String> getPartitionColumnName();

        @Config("other_consumer_configs")
        @ConfigDefault("{}")
        public Map<String, String> getOtherConsumerConfigs();

        @Config("value_subject_name_strategy")
        @ConfigDefault("null")
        public java.util.Optional<String> getValueSubjectNameStrategy();

        @Config("columns")
        public SchemaConfig getColumns();
    }

    private static Map<Integer, List<PartitionInfo>> assignments = new HashMap<>();

    @Override
    public ConfigDiff transaction(ConfigSource config,
            InputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        Schema schema = task.getColumns().toSchema();
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, task.getBrokers());
        KafkaConsumer<Bytes, Bytes> consumer = new KafkaConsumer<>(props, new BytesDeserializer(), new BytesDeserializer());
        int maxTaskCount = Runtime.getRuntime().availableProcessors() * 2;

        int taskIndex = 0;
        for (String topic : task.getTopics()) {
            for (PartitionInfo partitionInfo : consumer.partitionsFor(topic)) {
                List<PartitionInfo> list = assignments.getOrDefault(taskIndex, new ArrayList<>());
                list.add(partitionInfo);
                assignments.putIfAbsent(taskIndex, list);
                taskIndex += 1;
                taskIndex = taskIndex % maxTaskCount;
            }
        }
        int taskCount = Math.min(assignments.size(), Runtime.getRuntime().availableProcessors() * 2);

        return resume(task.dump(), schema, taskCount, control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            Schema schema, int taskCount,
            InputPlugin.Control control)
    {
        control.run(taskSource, schema, taskCount);
        return Exec.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource,
            Schema schema, int taskCount,
            List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TaskReport run(TaskSource taskSource,
            Schema schema, int taskIndex,
            PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, task.getBrokers());
        KafkaConsumer<String, ObjectNode> consumer = new KafkaConsumer<>(props, new StringDeserializer(), new KafkaJsonDeserializer());
        List<TopicPartition> topicPartitions = assignments.get(taskIndex).stream().map(
            partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
            .collect(
                Collectors.toList());
        consumer.assign(topicPartitions);

        BufferAllocator allocator = Exec.getBufferAllocator();
        PageBuilder pageBuilder = new PageBuilder(allocator, schema, output);

        consumer.seekToBeginning(topicPartitions);
        ConsumerRecords<String, ObjectNode> records = consumer.poll(Duration.ofSeconds(30));
        records.forEach(System.out::println);

        TaskReport taskReport = Exec.newTaskReport();
        return taskReport;
    }

    @Override
    public ConfigDiff guess(ConfigSource config)
    {
        return Exec.newConfigDiff();
    }
}
