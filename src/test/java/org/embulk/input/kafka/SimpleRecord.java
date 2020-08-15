package org.embulk.input.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SimpleRecord
{
  @JsonProperty
  private String id;

  @JsonProperty("int_item")
  private Integer intItem;

  @JsonProperty("varchar_item")
  private String varcharItem;

  @JsonCreator
  public SimpleRecord(@JsonProperty("id") String id, @JsonProperty("int_item") Integer intItem, @JsonProperty("varchar_item") String varcharItem)
  {
    this.id = id;
    this.intItem = intItem;
    this.varcharItem = varcharItem;
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
}
