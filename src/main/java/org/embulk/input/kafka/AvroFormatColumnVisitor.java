package org.embulk.input.kafka;

import com.google.common.collect.ImmutableMap;
import org.apache.avro.generic.GenericRecord;
import org.embulk.input.kafka.KafkaInputPlugin.PluginTask;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampParser;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AvroFormatColumnVisitor extends AbstractKafkaInputColumnVisitor<Object> implements ColumnVisitor
{
  static class UnsupportedValueTypeException extends RuntimeException
  {
    public UnsupportedValueTypeException(String message)
    {
      super(message);
    }
  }

  public AvroFormatColumnVisitor(PluginTask task, PageBuilder pageBuilder, TimestampParser[] timestampParsers)
  {
    super(task, pageBuilder, timestampParsers);
  }

  private GenericRecord genericRecord()
  {
    return (GenericRecord) recordValue;
  }

  @Override
  public void booleanColumn(Column column)
  {
    Object value = genericRecord().get(column.getName());
    if (value == null) {
      pageBuilder.setNull(column);
      return;
    }
    pageBuilder.setBoolean(column, (boolean) value);
  }

  @Override
  public void longColumn(Column column)
  {
    if (super.isKeyColumn(column)) {
      super.longColumnForKey(column);
      return;
    }

    if (isPartitionColumn(column)) {
      super.longColumnForPartition(column);
      return;
    }

    Object value = genericRecord().get(column.getName());
    if (value == null) {
      pageBuilder.setNull(column);
      return;
    }

    long longValue;
    if (value instanceof Long) {
      longValue = (Long) value;
    }
    else if (value instanceof Integer) {
      longValue = ((Integer) value).longValue();
    }
    else if (value instanceof Double) {
      longValue = ((Double) value).longValue();
    }
    else if (value instanceof Float) {
      longValue = ((Float) value).longValue();
    }
    else {
      throw new RuntimeException(String.format("Cannot cast value to long: column=%s value=%s", column, value));
    }
    pageBuilder.setLong(column, longValue);
  }

  @Override
  public void doubleColumn(Column column)
  {
    if (super.isKeyColumn(column)) {
      super.doubleColumnForKey(column);
      return;
    }

    Object value = genericRecord().get(column.getName());
    if (value == null) {
      pageBuilder.setNull(column);
      return;
    }

    double doubleValue;
    if (value instanceof Long) {
      doubleValue = ((Long) value).doubleValue();
    }
    else if (value instanceof Integer) {
      doubleValue = ((Integer) value).doubleValue();
    }
    else if (value instanceof Double) {
      doubleValue = (Double) value;
    }
    else if (value instanceof Float) {
      doubleValue = ((Float) value).doubleValue();
    }
    else {
      throw new RuntimeException(String.format("Cannot cast value to double: column=%s value=%s", column, value));
    }
    pageBuilder.setDouble(column, doubleValue);
  }

  @Override
  public void stringColumn(Column column)
  {
    if (super.isKeyColumn(column)) {
      super.stringColumnForKey(column);
      return;
    }

    Object value = genericRecord().get(column.getName());
    if (value == null) {
      pageBuilder.setNull(column);
      return;
    }

    pageBuilder.setString(column, value.toString());
  }

  @Override
  public void timestampColumn(Column column)
  {
    if (super.isKeyColumn(column)) {
      super.timestampColumnForKey(column);
      return;
    }

    Object value = genericRecord().get(column.getName());
    if (value == null) {
      pageBuilder.setNull(column);
      return;
    }

    Timestamp timestamp = timestampParsers[column.getIndex()].parse(value.toString());
    pageBuilder.setTimestamp(column, timestamp);
  }

  @Override
  public void jsonColumn(Column column)
  {
    Object value = genericRecord().get(column.getName());
    Value jsonValue = convertAvroToValue(value);
    pageBuilder.setJson(column, jsonValue);
  }

  private Value convertAvroToValue(Object avroValue)
  {
    if (avroValue instanceof GenericRecord) {
      ImmutableMap.Builder<Value, Value> builder = ImmutableMap.builder();
      GenericRecord record = (GenericRecord) avroValue;
      record.getSchema().getFields().forEach(field -> builder
          .put(convertAvroToValue(field.name()), convertAvroToValue(record.get(field.name()))));
      return ValueFactory.newMap(builder.build());
    }
    else if (avroValue instanceof List<?>) {
      List<Value> values = ((List<?>) avroValue).stream().map(this::convertAvroToValue)
          .collect(Collectors.toList());
      return ValueFactory.newArray(values);
    }
    else if (avroValue instanceof Map<?, ?>) {
      ImmutableMap.Builder<Value, Value> builder = ImmutableMap.builder();
      ((Map<?, ?>) avroValue).forEach((k, v) -> builder.put(convertAvroToValue(k), convertAvroToValue(v)));
      return ValueFactory.newMap(builder.build());
    }
    else if (avroValue instanceof Boolean) {
      return ValueFactory.newBoolean((Boolean) avroValue);
    }
    else if (avroValue instanceof Integer) {
      return ValueFactory.newInteger((Integer) avroValue);
    }
    else if (avroValue instanceof Long) {
      return ValueFactory.newInteger((Long) avroValue);
    }
    else if (avroValue instanceof Float) {
      return ValueFactory.newFloat((Float) avroValue);
    }
    else if (avroValue instanceof Double) {
      return ValueFactory.newFloat((Double) avroValue);
    }
    else if (avroValue instanceof String) {
      return ValueFactory.newString((String) avroValue);
    }
    else if (avroValue == null) {
      return ValueFactory.newNil();
    }
    else {
      throw new UnsupportedValueTypeException(String.format("%s is not supported", avroValue));
    }
  }
}
