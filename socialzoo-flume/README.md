## socialzoo-flume
Flume pipeline:

* Source: Twitter Streaming API source that encodes JSON to Avro according compatible with Confluent's SchemaRegistry
* Channel: Memory
* Sink: Default Kafka Sink that writes a byte[] to Kafka
