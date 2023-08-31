# Kafka input plugin for Embulk

Apache Kafka input plugin for Embulk

## Overview

* **Plugin type**: input
* **Resume supported**: yes
* **Cleanup supported**: no
* **Guess supported**: no

## Configuration

- **broker**: kafka broker host and port (array(string), required)
- **topics**: target topic name (array(string), required)
- **topic_column**: use column value as target topic (string, default: `null`)
- **schema_registry_url**: Schema Registy URL that is needed for avro format (string, default: `null`)
- **serialize_format**: use column value as target topic (enum, required, `json` or `avro_with_schema_registry`)
- **key_column_name**: use column value as record key. If `columns` option has same key, plugin set the record key to the column (string, default: `_key`)
- **partition_column_name**: use column value as partition id. If `columns` option has same key, plugin set the partition id of the record to the column (string, default: `_partition`)
- **fetch_max_wait_ms**: Max wait time for polling (integer, default: `30000`)
- **seek_mode**: Set seek mode to determine start offset (enum, default: `earliest`, availables: [`earliest`, `timestamp`, `offset`])
- **timestamp_for_seeking**: Timestamp to search seek position (timestamp, default: `null`)
- **offsets_for_seeking**: Offsets map to seek start offset (map(string, long), default: `null`, example: `{"topic-name:0": 12345, "topic-name:1": 23456}`)
- **max_empty_pollings**: If the result of pollings is empty over this value times, plugin give up to poll the partition (integer, default: `2`)
- **other_consumer_configs**: other consumer configs (json, default: `{}`)
- **value_subject_name_strategy**: Set SchemaRegistry subject name strategy (string, default: `null`, ex. `io.confluent.kafka.serializers.subject.RecordNameStrategy`)
- **columns**: embulk column configs (array(column config))

## Example

```yaml
type: kafka
topics:
  - "json-complex-topic"
serialize_format: json
brokers:
  - "localhost:9092"
fetch_max_wait_ms: 10000
columns:
  - {name: id, type: string}
  - {name: int_item, type: long}
  - {name: varchar_item, type: string}
  - {name: time, type: timestamp, format: "%Y-%m-%dT%H:%M:%SZ"}
  - {name: array, type: json}
  - {name: data, type: json}
  - {name: _key, type: string}
  - {name: _partition, type: long}
```


## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```
