# socialzoo-flume
Flume pipeline:

1. Source: Twitter Streaming API source that encodes JSON to Avro according compatible with Confluent's SchemaRegistry
2. Channel: Memory
3. Sink: Default Kafka Sink that writes a byte[] to Kafka

## Installation

Flume comes shipped with a TwitterSource by default which uses twitter4j v3. However, this project uses v4. To use our TwitterSource, we need to resolve these conflicts. Either create a _fat jar_ or delete the twitter source and twitter4j jars from the flume's jar/lib directory.

For CDH5 (parcel installed), these jars would be present at `/opt/cloudera/parcels/CDH/lib/flume-ng/lib/`:

1. flume-twitter-source-*.jar
2. twitter4j-*.jar

### Installation Steps

1. Create a directory in the `plugins.d` directory of flume (eg: `/var/lib/flume-ng/plugins.d`).
2. Create the directory structure like:
	```
	/var/lib/flume-ng/plugins.d\twitter-source\lib\socialzoo-flume-1.0-SNAPSHOT.jar
	/var/lib/flume-ng/plugins.d\twitter-source\libext\twitter4j-core-4.0.3.jar
	/var/lib/flume-ng/plugins.d\twitter-source\libext\twitter4j-stream-4.0.3.jar
	```
3. We use `Avro IDL` to create and compile the avro schema. The IDL file is located in a separate subproject `socialzoo-avro` under `src/main/idl/Tweet.avdl`. Compile the IDL to avsc file by executing a gradle task `compileIdl` on that project (which is also linked to assemble task). You only need to use `Tweet.avsc` for this source as that schema is self-sufficient.
4. Sample flume configuration
	```
	streaming1.sources = source1
	streaming1.channels = channel1
	streaming1.sinks = sink1
	
	streaming1.sources.source1.type = com.socialzoo.flume.TwitterSource
	streaming1.sources.source1.twitter.consumerKey = 
	streaming1.sources.source1.twitter.consumerSecret = 
	streaming1.sources.source1.twitter.accessToken = 
	streaming1.sources.source1.twitter.accessTokenSecret = 
	streaming1.sources.source1.twitter.track = pepsi, coke
	streaming1.sources.source1.batch.size = 100
	streaming1.sources.source1.batch.duration = 20000
	streaming1.sources.source1.interval.stats = 100
	streaming1.sources.source1.kafka.topic = tweets9
	streaming1.sources.source1.debug = true
	streaming1.sources.source1.schema.avro.url = hdfs:///user/gauravk/socialzoo/schema.avsc
	streaming1.sources.source1.schema.registry.url = http://localhost:8081
	streaming1.sources.source1.channels = channel1
	
	streaming1.channels.channel1.type = memory
	streaming1.channels.channel1.capacity = 5000
	streaming1.channels.channel1.transactionCapacity = 5000
	
	streaming1.sinks.sink1.type = org.apache.flume.sink.kafka.KafkaSink
	streaming1.sinks.sink1.topic = tweets9
	streaming1.sinks.sink1.brokerList = localhost:9092
	streaming1.sinks.sink1.batchSize = 100
	streaming1.sinks.sink1.requiredAcks = 0
	streaming1.sinks.sink1.kafka.producer.type = async
	streaming1.sinks.sink1.kafka.compression.codec = snappy
	streaming1.sinks.sink1.kafka.queue.buffering.max.ms = 20000
	streaming1.sinks.sink1.channel = channel1
	```

TwitterSource will do the following:

1. Automatically fetch the schema from the specified resource (HDFS/File/URL)
2. Register it with Confluent's schema registry for the given topic and get the global ID of the schema
3. Initiate the Twitter Streaming API.
4. For every JSON received, it will use the schema to extract the relevant fields out of the JSON and construct the Kafka payload with the schema ID prepended to it.
