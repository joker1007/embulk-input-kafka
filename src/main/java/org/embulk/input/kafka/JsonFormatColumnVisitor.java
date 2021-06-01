package org.embulk.input.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import org.embulk.input.kafka.KafkaInputPlugin.PluginTask;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.time.Timestamp;
import org.embulk.util.timestamp.TimestampFormatter;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonFormatColumnVisitor extends AbstractKafkaInputColumnVisitor<ObjectNode> implements ColumnVisitor
{
  public JsonFormatColumnVisitor(PluginTask task, PageBuilder pageBuilder, TimestampFormatter[] timestampFormatters)
  {
    super(task, pageBuilder, timestampFormatters);
  }

  @Override
  public void booleanColumn(Column column)
  {
     JsonNode value = recordValue.get(column.getName());
     if (value.isNull()) {
       pageBuilder.setNull(column);
       return;
     }

     pageBuilder.setBoolean(column, value.booleanValue());
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

    JsonNode value = recordValue.get(column.getName());
    if (value.isNull()) {
      pageBuilder.setNull(column);
      return;
    }

    pageBuilder.setLong(column, value.longValue());
  }

  @Override
  public void doubleColumn(Column column)
  {
    if (super.isKeyColumn(column)) {
      super.doubleColumnForKey(column);
      return;
    }

    JsonNode value = recordValue.get(column.getName());
    if (value.isNull()) {
      pageBuilder.setNull(column);
      return;
    }

    pageBuilder.setDouble(column, value.doubleValue());
  }

  @Override
  public void stringColumn(Column column)
  {
    if (super.isKeyColumn(column)) {
      super.stringColumnForKey(column);
      return;
    }

    JsonNode value = recordValue.get(column.getName());
    if (value.isNull()) {
      pageBuilder.setNull(column);
      return;
    }

    pageBuilder.setString(column, value.textValue());
  }

  @SuppressWarnings("deprecation")
  @Override
  public void timestampColumn(Column column)
  {
    if (super.isKeyColumn(column)) {
      super.timestampColumnForKey(column);
      return;
    }

    JsonNode value = recordValue.get(column.getName());
    if (value.isNull()) {
      pageBuilder.setNull(column);
      return;
    }

    Instant instant = timestampFormatters[column.getIndex()].parse(value.textValue());
    Timestamp timestamp = Timestamp.ofInstant(instant);
    pageBuilder.setTimestamp(column, timestamp);
  }

  @Override
  public void jsonColumn(Column column)
  {
    JsonNode jsonNode = recordValue.get(column.getName());
    if (jsonNode.isNull()) {
      pageBuilder.setNull(column);
      return;
    }

    pageBuilder.setJson(column, convertJsonValueToMsgpackValue(jsonNode));
  }

  private Value convertArrayToMsgpackValue(ArrayNode node)
  {
    List<Value> values = new ArrayList<>();
    node.forEach(item -> values.add(convertJsonValueToMsgpackValue(item)));
    return ValueFactory.newArray(values);
  }

  private Value convertObjectToMsgpackValue(ObjectNode node)
  {
    Map<Value, Value> values = new HashMap<>();
    node.fields().forEachRemaining(field -> {
      values.put(ValueFactory.newString(field.getKey()),
          convertJsonValueToMsgpackValue(field.getValue()));
    });
    return ValueFactory.newMap(values);
  }

  private Value convertJsonValueToMsgpackValue(JsonNode node)
  {
    if (node.isFloatingPointNumber()) {
      return ValueFactory.newFloat(node.doubleValue());
    }
    else if (node.isIntegralNumber()) {
      return ValueFactory.newInteger(node.longValue());
    }
    else if (node.isIntegralNumber()) {
      return ValueFactory.newInteger(node.longValue());
    }
    else if (node.isTextual()) {
      return ValueFactory.newString(node.textValue());
    }
    else if (node.isNull()) {
      return ValueFactory.newNil();
    }
    else if (node.isArray()) {
      return convertArrayToMsgpackValue((ArrayNode) node);
    }
    else if (node.isObject()) {
      return convertObjectToMsgpackValue((ObjectNode) node);
    }

    throw new RuntimeException("unknown json node");
  }
}
