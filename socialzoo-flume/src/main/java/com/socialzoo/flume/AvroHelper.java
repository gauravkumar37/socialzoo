package com.socialzoo.flume;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroHelper {
	private static final Logger LOGGER = LoggerFactory.getLogger(AvroHelper.class);
	private static Map<String, Schema> schemaCache = new HashMap<String, Schema>();

	public static Schema getAvroSchema(String schemaUrl) throws IOException {
		Schema schema = schemaCache.get(schemaUrl);
		if (schema == null) {
			schema = loadFromUrl(schemaUrl);
			schemaCache.put(schemaUrl, schema);
			LOGGER.info("Schema catched for url: " + schemaUrl);
			LOGGER.info("schemaCache contains keys: " + schemaCache.keySet());
		} else
			LOGGER.info("Schema loaded from cache for url: " + schemaUrl);
		return schema;
	}

	private static Schema loadFromUrl(String schemaUrl) throws IOException {
		LOGGER.info("Fetching schema from " + schemaUrl);
		Configuration conf = new Configuration();
		Schema.Parser parser = new Schema.Parser();
		if (schemaUrl.toLowerCase(Locale.ENGLISH).startsWith("hdfs://")) {
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream input = null;
			try {
				input = fs.open(new Path(schemaUrl));
				return parser.parse(input);
			} finally {
				if (input != null) {
					input.close();
				}
			}
		} else {
			InputStream is = null;
			try {
				is = new URL(schemaUrl).openStream();
				return parser.parse(is);
			} finally {
				if (is != null) {
					is.close();
				}
			}
		}
	}

	public static byte[] fromJsonToAvro(String json, Schema schema) throws IOException {
		InputStream input = new ByteArrayInputStream(json.getBytes());
		DataInputStream din = new DataInputStream(input);
		Decoder decoder = DecoderFactory.get().jsonDecoder(schema, din);
		DatumReader<Object> reader = new GenericDatumReader<Object>(schema);
		Object datum = reader.read(null, decoder);
		GenericDatumWriter<Object> w = new GenericDatumWriter<Object>(schema);
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		Encoder e = EncoderFactory.get().binaryEncoder(outputStream, null);
		w.write(datum, e);
		e.flush();
		return outputStream.toByteArray();
	}

}
