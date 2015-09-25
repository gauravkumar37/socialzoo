package com.socialzoo.flume;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterObjectFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterSource extends AbstractSource implements EventDrivenSource, Configurable, StatusListener {

	private TwitterStream twitterStream;

	private long eventCount = 0;
	private long startTime = 0;
	private long exceptionCount = 0;
	private long totalTextIndexed = 0;
	private long skippedDocs = 0;
	private long batchEndTime = 0;
	private List<Event> listEvents = Collections.emptyList();
	private HashMap<String, String> eventHeaders = null;
	private Schema avroSchema = null;
	protected static final byte MAGIC_BYTE = 0x0;
	protected static final int idSize = 4;
	protected static byte[] schemaIdArray = ByteBuffer.allocate(idSize).putInt(2).array();

	private int maxBatchSize = 1000;
	private int maxBatchDurationMillis = 1000;

	private DecimalFormat numFormatter = new DecimalFormat("###,###.###");
	private static int REPORT_INTERVAL = 5;
	private static int STATS_INTERVAL = REPORT_INTERVAL * 10;
	private static final Logger LOGGER = LoggerFactory.getLogger(TwitterSource.class);
	public static final String AVRO_SCHEMA_URL_PROPERTY = "avro.schema.url";
	public static final String AVRO_SCHEMA_URL_HEADER = "flume.avro.schema.url";

	public TwitterSource() {
	}

	@Override
	public void configure(Context context) {
		LOGGER.info("Configuring twitter source {} ...", this);
		String consumerKey = context.getString("consumerKey");
		String consumerSecret = context.getString("consumerSecret");
		String accessToken = context.getString("accessToken");
		String accessTokenSecret = context.getString("accessTokenSecret");
		maxBatchSize = context.getInteger("maxBatchSize", maxBatchSize);
		maxBatchDurationMillis = context.getInteger("maxBatchDurationMillis", maxBatchDurationMillis);

		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setJSONStoreEnabled(true);
		cb.setGZIPEnabled(true);
		twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
		twitterStream.setOAuthConsumer(consumerKey, consumerSecret);
		twitterStream.setOAuthAccessToken(new AccessToken(accessToken, accessTokenSecret));

		String avroSchemaURL = context.getString(AVRO_SCHEMA_URL_PROPERTY);
		if (avroSchemaURL == null)
			throw new ConfigurationException("Missing required property: " + AVRO_SCHEMA_URL_PROPERTY);
		try {
			avroSchema = AvroHelper.getAvroSchema(avroSchemaURL);
		} catch (IOException e) {
			throw new ConfigurationException("Exception while loading schema from " + AVRO_SCHEMA_URL_PROPERTY, e);
		}
		eventHeaders = new HashMap<String, String>();
		eventHeaders.put(AVRO_SCHEMA_URL_HEADER, avroSchemaURL);

		listEvents = new ArrayList<Event>((int) Math.floor(maxBatchSize * 1.5));
	}

	@Override
	public synchronized void start() {
		LOGGER.info("Starting twitter source {} ...", this);
		eventCount = 0;
		startTime = System.currentTimeMillis();
		exceptionCount = 0;
		totalTextIndexed = 0;
		skippedDocs = 0;
		batchEndTime = System.currentTimeMillis() + maxBatchDurationMillis;
		twitterStream.addListener(this);
		twitterStream.filter(new FilterQuery(0, null, new String[] { "android" }, null, new String[] { "en" }));
		LOGGER.info("Twitter source {} started.", getName());
		super.start();
	}

	public void onStatus(Status status) {
		String json = TwitterObjectFactory.getRawJSON(status);
		LOGGER.debug(json);
		try {
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			out.write(MAGIC_BYTE);
			out.write(schemaIdArray);
			out.write(AvroHelper.fromJsonToAvro(json, avroSchema));

			listEvents.add(EventBuilder.withBody(out.toByteArray(), eventHeaders));
			out.close();
			eventCount++;
			if (listEvents.size() >= maxBatchSize || System.currentTimeMillis() >= batchEndTime) {
				LOGGER.info("Flushing => listEvents.size: " + listEvents.size() + ", eventCount: " + eventCount);
				getChannelProcessor().processEventBatch(listEvents);
				listEvents.clear();
				batchEndTime = System.currentTimeMillis() + maxBatchDurationMillis;
				LOGGER.info("Flushed => listEvents.size: " + listEvents.size() + ", eventCount: " + eventCount);
			}
		} catch (IOException e) {
			LOGGER.error("Error: ", e);
		}
		if ((eventCount % REPORT_INTERVAL) == 0) {
			LOGGER.info(String.format("Processed %s docs", numFormatter.format(eventCount)));
		}
		if ((eventCount % STATS_INTERVAL) == 0) {
			logStats();
		}
	}

	private void logStats() {
		double mbIndexed = totalTextIndexed / (1024 * 1024.0);
		long seconds = (System.currentTimeMillis() - startTime) / 1000;
		seconds = Math.max(seconds, 1);
		LOGGER.info(String.format("Total docs indexed: %s, total skipped docs: %s", numFormatter.format(eventCount),
				numFormatter.format(skippedDocs)));
		LOGGER.info(String.format("    %s docs/second", numFormatter.format(eventCount / seconds)));
		LOGGER.info(String.format("Run took %s seconds and processed:", numFormatter.format(seconds)));
		LOGGER.info(String.format("    %s MB/sec sent to index",
				numFormatter.format(((float) totalTextIndexed / (1024 * 1024)) / seconds)));
		LOGGER.info(String.format("    %s MB text sent to index", numFormatter.format(mbIndexed)));
		LOGGER.info(String.format("There were %s exceptions ignored: ", numFormatter.format(exceptionCount)));
	}

	@Override
	public synchronized void stop() {
		LOGGER.info("Twitter source {} stopping...", getName());
		twitterStream.shutdown();
		twitterStream = null;
		listEvents = null;
		eventHeaders = null;
		LOGGER.info("Twitter source {} stopped.", getName());
		super.stop();
	}

	public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
		// Do nothing...
	}

	public void onScrubGeo(long userId, long upToStatusId) {
		// Do nothing...
	}

	public void onStallWarning(StallWarning warning) {
		// Do nothing...
	}

	public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
		// Do nothing...
	}

	public void onException(Exception e) {
		LOGGER.error("Exception while streaming tweets", e);
	}
}
