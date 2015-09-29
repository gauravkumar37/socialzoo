package com.socialzoo.flume;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.commons.lang.time.DurationFormatUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
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

	private long startTime = 0;
	private long totalTextIndexed = 0;
	private long totalSkippedDocs = 0;
	private long totalIndexedDocs = 0;
	private long batchEndTime = 0;
	private TwitterStream twitterStream;
	private String[] trackKeywords = null;
	private Schema schema = null;
	private byte[] schemaIdArray = null;
	private List<Event> listEvents = Collections.emptyList();
	private final DecimalFormat numFormatter = new DecimalFormat("###,###.##");
	private final Logger LOGGER = LoggerFactory.getLogger(TwitterSource.class);

	// Flume config properties
	private int maxBatchSize = 1000;
	private int maxBatchDurationMillis = 1000;
	private int intervalStats = 50;
	public final String INTERVAL_REPORT = "interval.report";
	public final String INTERVAL_STATS = "interval.stats";
	public final String MAX_BATCH_SIZE = "batch.size";
	public final String MAX_BATCH_MILLIS = "batch.duration";

	public final String KAFKA_TOPIC = "kafka.topic";

	public final String AVRO_SCHEMA_URL = "schema.avro.url";
	public final String SCHEMA_REGISTRY_URL = "schema.registry.url";

	public final String TWITTER_PREFIX = "twitter.";
	public final String TWITTER_TRACK = "track";
	public final String TWITTER_CONSUMER_KEY = "consumerKey";
	public final String TWITTER_CONSUMER_SECRET = "consumerSecret";
	public final String TWITTER_ACCESS_TOKEN = "accessToken";
	public final String TWITTER_ACCESS_SECRET = "accessTokenSecret";
	private boolean debug;

	public TwitterSource() {
	}

	@Override
	public void configure(Context context) {
		LOGGER.info("Configuring twitter source {} ...", this);
		ImmutableMap<String, String> twitterProps = context.getSubProperties(TWITTER_PREFIX);
		if (twitterProps == null)
			throw new ConfigurationException("Twitter oauth tokens missing from config.");
		String consumerKey = twitterProps.get(TWITTER_CONSUMER_KEY);
		String consumerSecret = twitterProps.get(TWITTER_CONSUMER_SECRET);
		String accessToken = twitterProps.get(TWITTER_ACCESS_TOKEN);
		String accessTokenSecret = twitterProps.get(TWITTER_ACCESS_SECRET);
		if (consumerKey == null || consumerSecret == null || accessToken == null || accessTokenSecret == null)
			throw new ConfigurationException("Twitter oauth tokens missing from config.");
		ConfigurationBuilder cb = new ConfigurationBuilder();
		if (context.getBoolean("debug", false)) {
			debug = true;
			cb.setJSONStoreEnabled(true);
		}
		cb.setGZIPEnabled(true);
		twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
		twitterStream.setOAuthConsumer(consumerKey, consumerSecret);
		twitterStream.setOAuthAccessToken(new AccessToken(accessToken, accessTokenSecret));
		if (twitterProps.get(TWITTER_TRACK) != null && !twitterProps.get(TWITTER_TRACK).trim().equals("")) {
			List<String> listTrackKeywords = new LinkedList<String>();
			for (String keyword : twitterProps.get(TWITTER_TRACK).trim().split(",")) {
				if (!keyword.trim().equals(""))
					listTrackKeywords.add(keyword.trim());
			}
			trackKeywords = (String[]) listTrackKeywords.toArray(new String[listTrackKeywords.size()]);
			LOGGER.info("Twitter keywords to track: {}", Arrays.asList(trackKeywords));
		}

		LOGGER.info("Configuring schema registry {} ...", this);
		try {
			if (context.getString(SCHEMA_REGISTRY_URL) == null || context.getString(KAFKA_TOPIC) == null
					|| context.getString(AVRO_SCHEMA_URL) == null)
				throw new ConfigurationException("Missing required schema related properties.");
			CachedSchemaRegistryClient cachedSchemaRegistryClient = new CachedSchemaRegistryClient(
					context.getString(SCHEMA_REGISTRY_URL), 20);
			schema = AvroHelper.loadSchemaFromUrl(context.getString(AVRO_SCHEMA_URL));
			int schemaId = cachedSchemaRegistryClient.register(context.getString(KAFKA_TOPIC) + "-value", schema);
			LOGGER.info("schemaId: {}", schemaId);
			schemaIdArray = ByteBuffer.allocate(4).putInt(schemaId).array();
		} catch (IOException | RestClientException e) {
			LOGGER.error("Error while instantiating schema registry client.", e);
			throw new ConfigurationException("Error while instantiating schema registry client.");
		}

		maxBatchSize = context.getInteger(MAX_BATCH_SIZE, maxBatchSize);
		maxBatchDurationMillis = context.getInteger(MAX_BATCH_MILLIS, maxBatchDurationMillis);
		intervalStats = context.getInteger(INTERVAL_STATS, intervalStats);
		listEvents = new ArrayList<Event>((int) Math.floor(maxBatchSize));
	}

	@Override
	public synchronized void start() {
		LOGGER.info("Starting twitter source {} ...", this);
		startTime = System.currentTimeMillis();
		batchEndTime = startTime + maxBatchDurationMillis;
		twitterStream.addListener(this);
		if (trackKeywords != null)
			twitterStream.filter(new FilterQuery(0, null, trackKeywords, null, new String[] { "en" }));
		LOGGER.info("Twitter source {} started.", getName());
		super.start();
	}

	public void onStatus(Status status) {
		String rawJSON = null;
		if (debug)
			rawJSON = TwitterObjectFactory.getRawJSON(status);
		try {
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			out.write((byte) 0x0);
			out.write(schemaIdArray);
			AvroHelper.tweetToAvro(out, status, schema);
			listEvents.add(EventBuilder.withBody(out.toByteArray()));
			totalIndexedDocs++;
			totalTextIndexed += out.size() - 5;
			out.close();
			if ((totalIndexedDocs % intervalStats) == 0)
				logStats();
			if (listEvents.size() >= maxBatchSize || System.currentTimeMillis() >= batchEndTime) {
				LOGGER.info("Flushing {} events to channel.", listEvents.size());
				getChannelProcessor().processEventBatch(listEvents);
				listEvents.clear();
				batchEndTime = System.currentTimeMillis() + maxBatchDurationMillis;
			}
		} catch (Exception e) {
			totalSkippedDocs++;
			LOGGER.info("totalSkippedDocs: {}", totalSkippedDocs);
			if (rawJSON != null) {
				LOGGER.error("Exception encountered. rawJSON: " + rawJSON + System.lineSeparator() + "Status:"
						+ status.toString(), e);
			} else
				LOGGER.error("Exception encountered with status: " + status.toString(), e);
		}
	}

	private void logStats() {
		double mbIndexed = totalTextIndexed / (1024 * 1024.0);
		long seconds = (System.currentTimeMillis() - startTime) / 1000;
		LOGGER.info(
				"Total runtime: {}, Events indexed: {}, Events skipped: {}, MBs indexed: {}, "
						+ "Speed: {} indexed events/sec, {} indexed MBps",
				DurationFormatUtils.formatDuration(seconds * 1000, "HH:mm:ss"), numFormatter.format(totalIndexedDocs),
				numFormatter.format(totalSkippedDocs), numFormatter.format(mbIndexed),
				numFormatter.format((float) totalIndexedDocs / seconds),
				numFormatter.format((float) mbIndexed / seconds));
	}

	@Override
	public synchronized void stop() {
		LOGGER.info("Twitter source {} stopping...", getName());
		twitterStream.shutdown();
		twitterStream = null;
		listEvents = null;
		LOGGER.info("Twitter source {} stopped.", getName());
		super.stop();
	}

	public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
		LOGGER.info("DeletionNotice: {}", statusDeletionNotice.toString());
	}

	public void onScrubGeo(long userId, long upToStatusId) {
		LOGGER.info("ScrubGeo: {}, {}", userId, upToStatusId);
	}

	public void onStallWarning(StallWarning warning) {
		LOGGER.warn("StallWarning: {}", warning.toString());
	}

	public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
		LOGGER.warn("TrackLimitationNotice: {}", numberOfLimitedStatuses);
	}

	public void onException(Exception e) {
		LOGGER.error("Exception while streaming tweets.", e);
	}
}
