package org.embulk.input.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.List;
import java.util.Map;

public class ComplexRecord
{
  @JsonProperty
  private String id;

  @JsonProperty("int_item")
  private Integer intItem;

  @JsonProperty("varchar_item")
  private String varcharItem;

  @JsonProperty("time")
  private Instant time;

  @JsonProperty("array")
  private List<String> array;

  @JsonProperty("data")
  private Map<String, Map<String, String>> data;

  @JsonCreator
  public ComplexRecord(@JsonProperty("id") String id, @JsonProperty("int_item") Integer intItem,
      @JsonProperty("varchar_item") String varcharItem, @JsonProperty("time") Instant time,
      @JsonProperty("array") List<String> array,
      @JsonProperty("data") Map<String, Map<String, String>> data)
  {
    this.id = id;
    this.intItem = intItem;
    this.varcharItem = varcharItem;
    this.time = time;
    this.array = array;
    this.data = data;
  }

  public String getId()
  {
    return id;
  }

  public Integer getIntItem()
  {
    return intItem;
  }

  public String getVarcharItem()
  {
    return varcharItem;
  }

  public Instant getTime()
  {
    return time;
  }

  public List<String> getArray()
  {
    return array;
  }

  public Map<String, Map<String, String>> getData()
  {
    return data;
  }

}
