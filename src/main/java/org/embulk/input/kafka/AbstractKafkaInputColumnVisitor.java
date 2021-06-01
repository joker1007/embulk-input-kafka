package org.embulk.input.kafka;

import java.time.Instant;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Bytes;
import org.embulk.input.kafka.KafkaInputPlugin.PluginTask;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.time.Timestamp;
import org.embulk.util.timestamp.TimestampFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractKafkaInputColumnVisitor<V> implements ColumnVisitor
{
  protected final PluginTask task;
  protected final PageBuilder pageBuilder;
  protected final TimestampFormatter[] timestampFormatters;

  protected ConsumerRecord<Bytes, V> consumerRecord;
  protected V recordValue;

  private static final LongDeserializer longDeserializer = new LongDeserializer();
  private static final StringDeserializer stringDeserializer = new StringDeserializer();
  private static final DoubleDeserializer doubleDeserializer = new DoubleDeserializer();

  private static final Logger logger = LoggerFactory.getLogger(AbstractKafkaInputColumnVisitor.class);

  public AbstractKafkaInputColumnVisitor(PluginTask task, PageBuilder pageBuilder, TimestampFormatter[] timestampFormatters)
  {
    this.task = task;
    this.pageBuilder = pageBuilder;
    this.timestampFormatters = timestampFormatters;
  }

  public void reset(ConsumerRecord<Bytes, V> consumerRecord)
  {
    this.consumerRecord = consumerRecord;
    this.recordValue = consumerRecord.value();
  }

  protected boolean isKeyColumn(Column column)
  {
    return column.getName().equals(task.getKeyColumnName());
  }

  protected boolean isPartitionColumn(Column column)
  {
    return column.getName().equals(task.getPartitionColumnName());
  }

  protected void longColumnForKey(Column column)
  {
    if (consumerRecord.serializedKeySize() == 0) {
      pageBuilder.setNull(column);
      return;
    }
    try {
      long key = longDeserializer.deserialize("dummy", consumerRecord.key().get());
      pageBuilder.setLong(column, key);
    }
    catch (Exception e) {
      logDeserializeFailure(consumerRecord);
      pageBuilder.setNull(column);
    }
  }

  protected void longColumnForPartition(Column column)
  {
    pageBuilder.setLong(column, Integer.valueOf(consumerRecord.partition()).longValue());
  }

  public void doubleColumnForKey(Column column)
  {
    if (consumerRecord.serializedKeySize() == 0) {
      pageBuilder.setNull(column);
      return;
    }
    try {
      double key = doubleDeserializer.deserialize("dummy", consumerRecord.key().get());
      pageBuilder.setDouble(column, key);
    }
    catch (Exception e) {
      logDeserializeFailure(consumerRecord);
      pageBuilder.setNull(column);
    }
  }

  public void stringColumnForKey(Column column)
  {
    if (consumerRecord.serializedKeySize() == 0) {
      pageBuilder.setNull(column);
      return;
    }
    try {
      String key = stringDeserializer.deserialize("dummy", consumerRecord.key().get());
      pageBuilder.setString(column, key);
    }
    catch (Exception e) {
      logDeserializeFailure(consumerRecord);
      pageBuilder.setNull(column);
    }
  }

  @SuppressWarnings("deprecation")
  public void timestampColumnForKey(Column column)
  {
    if (consumerRecord.serializedKeySize() == 0) {
      pageBuilder.setNull(column);
      return;
    }
    try {
      String key = stringDeserializer.deserialize("dummy", consumerRecord.key().get());
      try {
        Instant instant = timestampFormatters[column.getIndex()].parse(key);
        Timestamp timestamp = Timestamp.ofInstant(instant);
        pageBuilder.setTimestamp(column, timestamp);
      }
      catch (Exception e) {
        logDeserializeFailure(consumerRecord);
        pageBuilder.setNull(column);
      }
    }
    catch (Exception e) {
      logDeserializeFailure(consumerRecord);
      pageBuilder.setNull(column);
    }
  }

  private void logDeserializeFailure(ConsumerRecord<Bytes, V> consumerRecord)
  {
    logger.warn("Failed to deserialize record key: key={} value={}", consumerRecord.key(), consumerRecord.value());
  }
}
