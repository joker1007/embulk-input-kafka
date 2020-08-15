package org.embulk.input.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampParser;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonFormatColumnVisitor implements ColumnVisitor
{
  private final PageBuilder pageBuilder;
  private final TimestampParser[] timestampParsers;
  private ObjectNode currentObjectNode;

  public JsonFormatColumnVisitor(PageBuilder pageBuilder, TimestampParser[] timestampParsers)
  {
    this.pageBuilder = pageBuilder;
    this.timestampParsers = timestampParsers;
  }

  public void reset(ObjectNode objectNode)
  {
    this.currentObjectNode = objectNode;
  }

  @Override
  public void booleanColumn(Column column)
  {
     boolean value = currentObjectNode.get(column.getName()).booleanValue();
     pageBuilder.setBoolean(column, value);
  }

  @Override
  public void longColumn(Column column)
  {
    long value = currentObjectNode.get(column.getName()).longValue();
    pageBuilder.setLong(column, value);
  }

  @Override
  public void doubleColumn(Column column)
  {
    double value = currentObjectNode.get(column.getName()).doubleValue();
    pageBuilder.setDouble(column, value);
  }

  @Override
  public void stringColumn(Column column)
  {
    String value = currentObjectNode.get(column.getName()).textValue();
    pageBuilder.setString(column, value);
  }

  @Override
  public void timestampColumn(Column column)
  {
    String value = currentObjectNode.get(column.getName()).textValue();
    Timestamp timestamp = timestampParsers[column.getIndex()].parse(value);
    pageBuilder.setTimestamp(column, timestamp);
  }

  @Override
  public void jsonColumn(Column column)
  {
    JsonNode jsonNode = currentObjectNode.get(column.getName());
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
