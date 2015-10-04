package com.socialzoo.flume;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;
import twitter4j.ExtendedMediaEntity.Variant;
import twitter4j.MediaEntity.Size;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.*;

public class AvroHelper {
	private static final Logger LOGGER = LoggerFactory.getLogger(AvroHelper.class);

	public static Schema loadSchemaFromUrl(String schemaUrl) throws IOException {
		LOGGER.info("Fetching schema from {}", schemaUrl);
		Configuration conf = new Configuration();
		Schema.Parser parser = new Schema.Parser();
		InputStream is = null;
		try {
			if (schemaUrl.toLowerCase().startsWith("hdfs://")) {
				FileSystem fs = FileSystem.get(conf);
				is = fs.open(new Path(schemaUrl));
			} else {
				is = new URL(schemaUrl).openStream();
			}
			Schema schema = parser.parse(is);
			LOGGER.debug("Fetched schema from {}: {}", schemaUrl, schema);
			return schema;
		} finally {
			if (is != null) {
				is.close();
			}
		}
	}

	public static void tweetToAvro(ByteArrayOutputStream out, Status status, Schema schema) throws IOException {
		GenericDatumWriter<GenericRecord> gdw = new GenericDatumWriter<>(schema);
		Encoder e = EncoderFactory.get().binaryEncoder(out, null);
		GenericRecord tweet = buildTweet(schema, status);
		gdw.write(tweet, e);
		e.flush();
	}

	public static Record buildTweet(Schema schema, Status status) {
		GenericRecordBuilder builderTweet = new GenericRecordBuilder(schema);

		builderTweet.set("created_at", status.getCreatedAt().getTime());
		builderTweet.set("favorite_count", status.getFavoriteCount());
		builderTweet.set("favorited", status.isFavorited());
		builderTweet.set("id", status.getId());
		builderTweet.set("in_reply_to_screen_name", status.getInReplyToScreenName());
		if (status.getInReplyToStatusId() != -1)
			builderTweet.set("in_reply_to_status_id", status.getInReplyToStatusId());
		if (status.getInReplyToUserId() != -1)
			builderTweet.set("in_reply_to_user_id", status.getInReplyToUserId());
		builderTweet.set("lang", status.getLang());
		builderTweet.set("possibly_sensitive", status.isPossiblySensitive());
		builderTweet.set("retweet_count", status.getRetweetCount());
		builderTweet.set("retweeted", status.isRetweeted());
		builderTweet.set("source", status.getSource());
		builderTweet.set("text", status.getText());
		builderTweet.set("truncated", status.isTruncated());
		if (status.getWithheldInCountries() != null)
			builderTweet.set("withheld_in_countries", Arrays.asList(status.getWithheldInCountries()));
		if (status.getGeoLocation() != null)
			builderTweet.set("coordinates",
					Arrays.asList(status.getGeoLocation().getLatitude(), status.getGeoLocation().getLongitude()));

		builderTweet.set("entities", buildEntities(schema.getField("entities").schema(), status));

		if (status.getPlace() != null)
			builderTweet.set("place",
					buildPlace(schema.getField("place").schema().getTypes().get(1), status.getPlace()));

		User user = status.getUser();
		if (user != null && schema.getField("user") != null) {
			Schema schemaUser = schema.getField("user").schema();
			GenericRecordBuilder builderUser = new GenericRecordBuilder(schemaUser);
			builderUser.set("contributors_enabled", user.isContributorsEnabled());
			builderUser.set("created_at", user.getCreatedAt().getTime());
			builderUser.set("default_profile", user.isDefaultProfile());
			builderUser.set("default_profile_image", user.isDefaultProfileImage());
			builderUser.set("description", user.getDescription());
			builderUser.set("entities", buildURLEntity(schemaUser.getField("entities").schema(), user.getURLEntity()));
			builderUser.set("favourites_count", user.getFavouritesCount());
			builderUser.set("followers_count", user.getFollowersCount());
			builderUser.set("friends_count", user.getFriendsCount());
			builderUser.set("geo_enabled", user.isGeoEnabled());
			builderUser.set("id", user.getId());
			builderUser.set("is_translator", user.isTranslator());
			builderUser.set("lang", user.getLang());
			builderUser.set("listed_count", user.getListedCount());
			builderUser.set("location", user.getLocation());
			builderUser.set("name", user.getName());
			builderUser.set("screen_name", user.getScreenName());
			builderUser.set("profile_background_color", user.getProfileBackgroundColor());
			builderUser.set("profile_background_image_url", user.getProfileBackgroundImageURL());
			builderUser.set("profile_background_image_url_https", user.getProfileBackgroundImageUrlHttps());
			builderUser.set("profile_background_tile", user.isProfileBackgroundTiled());
			builderUser.set("profile_banner_url", user.getProfileBannerURL());
			builderUser.set("profile_image_url", user.getProfileImageURL());
			builderUser.set("profile_image_url_https", user.getProfileBackgroundImageUrlHttps());
			builderUser.set("profile_link_color", user.getProfileLinkColor());
			builderUser.set("profile_sidebar_border_color", user.getProfileSidebarBorderColor());
			builderUser.set("profile_sidebar_fill_color", user.getProfileSidebarFillColor());
			builderUser.set("profile_text_color", user.getProfileTextColor());
			builderUser.set("profile_use_background_image", user.isProfileUseBackgroundImage());
			builderUser.set("protected", user.isProtected());
			builderUser.set("show_all_inline_media", user.isShowAllInlineMedia());
			builderUser.set("statuses_count", user.getStatusesCount());
			builderUser.set("time_zone", user.getTimeZone());
			builderUser.set("url", user.getURL());
			builderUser.set("utc_offset", user.getUtcOffset());
			builderUser.set("verified", user.isVerified());
			if (user.getStatus() != null && schemaUser.getField("status") != null)
				builderUser.set("status",
						buildTweet(schemaUser.getField("status").schema().getTypes().get(1), user.getStatus()));
			if (user.getWithheldInCountries() != null)
				builderUser.set("withheld_in_countries", Arrays.asList(user.getWithheldInCountries()));
			builderTweet.set("user", builderUser.build());
		}

		if (status.getQuotedStatus() != null && schema.getField("quoted_status") != null)
			builderTweet.set("quoted_status",
					buildTweet(schema.getField("quoted_status").schema().getTypes().get(1), status.getQuotedStatus()));

		if (status.getRetweetedStatus() != null && schema.getField("retweeted_status") != null)
			builderTweet.set("retweeted_status", buildTweet(
					schema.getField("retweeted_status").schema().getTypes().get(1), status.getRetweetedStatus()));

		return builderTweet.build();
	}

	private static Record buildEntities(Schema schemaEntities, Status status) {
		GenericRecordBuilder builderEntities = new GenericRecordBuilder(schemaEntities);

		if (status.getHashtagEntities().length > 0) {
			Schema schemaHashtagObject = schemaEntities.getField("hashtags").schema().getElementType();
			List<GenericRecord> listHashtagObjects = new ArrayList<>();
			for (HashtagEntity hashtagEntity : status.getHashtagEntities()) {
				GenericRecordBuilder builderHashtagObject = new GenericRecordBuilder(schemaHashtagObject);
				builderHashtagObject.set("text", hashtagEntity.getText());
				builderHashtagObject.set("start", hashtagEntity.getStart());
				builderHashtagObject.set("end", hashtagEntity.getEnd());
				listHashtagObjects.add(builderHashtagObject.build());
			}
			builderEntities.set("hashtags", listHashtagObjects);
		} else
			builderEntities.set("hashtags", Collections.emptyList());

		if (status.getSymbolEntities().length > 0) {
			Schema schemaSymbolObject = schemaEntities.getField("symbols").schema().getElementType();
			List<GenericRecord> listSymbolObject = new ArrayList<>();
			for (SymbolEntity symbolEntity : status.getSymbolEntities()) {
				GenericRecordBuilder builderSymbolObject = new GenericRecordBuilder(schemaSymbolObject);
				builderSymbolObject.set("text", symbolEntity.getText());
				builderSymbolObject.set("start", symbolEntity.getStart());
				builderSymbolObject.set("end", symbolEntity.getEnd());
				listSymbolObject.add(builderSymbolObject.build());
			}
			builderEntities.set("symbols", listSymbolObject);
		} else
			builderEntities.set("symbols", Collections.emptyList());

		if (status.getMediaEntities().length > 0) {
			Schema schemaMediaObject = schemaEntities.getField("media").schema().getElementType();
			List<GenericRecord> listMediaObject = new ArrayList<>();
			for (MediaEntity mediaEntity : status.getMediaEntities()) {
				GenericRecordBuilder builderMediaObject = new GenericRecordBuilder(schemaMediaObject);
				builderMediaObject.set("url", mediaEntity.getURL());
				builderMediaObject.set("display_url", mediaEntity.getDisplayURL());
				builderMediaObject.set("expanded_url", mediaEntity.getExpandedURL());
				builderMediaObject.set("id", mediaEntity.getId());
				builderMediaObject.set("media_url", mediaEntity.getMediaURL());
				builderMediaObject.set("media_url_https", mediaEntity.getMediaURLHttps());
				builderMediaObject.set("type", mediaEntity.getType());
				builderMediaObject.set("text", mediaEntity.getText());
				builderMediaObject.set("start", mediaEntity.getStart());
				builderMediaObject.set("end", mediaEntity.getEnd());

				Schema schemaSize = schemaMediaObject.getField("sizes").schema().getValueType();
				GenericRecordBuilder builderSize = new GenericRecordBuilder(schemaSize);
				Map<String, GenericRecord> mapSizes = new HashMap<>(4);
				for (int key : mediaEntity.getSizes().keySet()) {
					Size size = mediaEntity.getSizes().get(key);
					builderSize.set("h", size.getHeight());
					builderSize.set("w", size.getWidth());
					builderSize.set("resize", size.getResize());
					mapSizes.put(Integer.toString(key), builderSize.build());
				}
				builderMediaObject.set("sizes", mapSizes);
				listMediaObject.add(builderMediaObject.build());
			}
			builderEntities.set("media", listMediaObject);
		} else
			builderEntities.set("media", Collections.emptyList());

		if (status.getURLEntities().length > 0) {
			Schema schemaURLObject = schemaEntities.getField("urls").schema().getElementType();
			List<GenericRecord> listURLObject1 = new ArrayList<>();
			for (URLEntity urlEntity : status.getURLEntities())
				listURLObject1.add(buildURLEntity(schemaURLObject, urlEntity));
			builderEntities.set("urls", listURLObject1);
		} else
			builderEntities.set("urls", Collections.emptyList());

		if (status.getUserMentionEntities().length > 0) {
			Schema schemaUserMentionObject = schemaEntities.getField("user_mentions").schema().getElementType();
			List<GenericRecord> listUserMentionObject = new ArrayList<>();
			for (UserMentionEntity userMentionEntity : status.getUserMentionEntities()) {
				GenericRecordBuilder builderUserMentionObject = new GenericRecordBuilder(schemaUserMentionObject);
				builderUserMentionObject.set("name", userMentionEntity.getName());
				builderUserMentionObject.set("screen_name", userMentionEntity.getScreenName());
				builderUserMentionObject.set("text", userMentionEntity.getText());
				builderUserMentionObject.set("id", userMentionEntity.getId());
				builderUserMentionObject.set("start", userMentionEntity.getStart());
				builderUserMentionObject.set("end", userMentionEntity.getEnd());
				listUserMentionObject.add(builderUserMentionObject.build());
			}
			builderEntities.set("user_mentions", listUserMentionObject);
		} else
			builderEntities.set("user_mentions", Collections.emptyList());

		if (status.getExtendedMediaEntities().length > 0) {
			Schema schemaExtendedMediaObject = schemaEntities.getField("extended_entities").schema().getElementType();
			List<GenericRecord> listExtendedMediaObject = new ArrayList<>();
			for (ExtendedMediaEntity extendedMediaEntity : status.getExtendedMediaEntities()) {
				GenericRecordBuilder builderExtendedMediaObject = new GenericRecordBuilder(schemaExtendedMediaObject);
				builderExtendedMediaObject.set("url", extendedMediaEntity.getURL());
				builderExtendedMediaObject.set("display_url", extendedMediaEntity.getDisplayURL());
				builderExtendedMediaObject.set("expanded_url", extendedMediaEntity.getExpandedURL());
				builderExtendedMediaObject.set("id", extendedMediaEntity.getId());
				builderExtendedMediaObject.set("media_url", extendedMediaEntity.getMediaURL());
				builderExtendedMediaObject.set("media_url_https", extendedMediaEntity.getMediaURLHttps());
				builderExtendedMediaObject.set("type", extendedMediaEntity.getType());
				builderExtendedMediaObject.set("text", extendedMediaEntity.getText());
				builderExtendedMediaObject.set("start", extendedMediaEntity.getStart());
				builderExtendedMediaObject.set("end", extendedMediaEntity.getEnd());

				Schema schemaSize = schemaExtendedMediaObject.getField("sizes").schema().getValueType();
				GenericRecordBuilder builderSize = new GenericRecordBuilder(schemaSize);
				Map<String, GenericRecord> mapSizes = new HashMap<>(4);
				for (int key : extendedMediaEntity.getSizes().keySet()) {
					Size size = extendedMediaEntity.getSizes().get(key);
					builderSize.set("h", size.getHeight());
					builderSize.set("w", size.getWidth());
					builderSize.set("resize", size.getResize());
					mapSizes.put(Integer.toString(key), builderSize.build());
				}
				builderExtendedMediaObject.set("sizes", mapSizes);

				Schema schemaVideoInfo = schemaExtendedMediaObject.getField("video_info").schema();
				GenericRecordBuilder builderVideoInfo = new GenericRecordBuilder(schemaVideoInfo);
				builderVideoInfo.set("h", extendedMediaEntity.getVideoAspectRatioHeight());
				builderVideoInfo.set("w", extendedMediaEntity.getVideoAspectRatioWidth());
				builderVideoInfo.set("duration_millis", extendedMediaEntity.getVideoDurationMillis());

				Schema schemaVideoVariants = schemaVideoInfo.getField("variants").schema().getElementType();
				List<GenericRecord> listVideoVariants = new ArrayList<>();
				for (Variant extendedVideoVariant : extendedMediaEntity.getVideoVariants()) {
					GenericRecordBuilder builderVideoVariant = new GenericRecordBuilder(schemaVideoVariants);
					builderVideoVariant.set("bitrate", extendedVideoVariant.getBitrate());
					builderVideoVariant.set("content_type", extendedVideoVariant.getContentType());
					builderVideoVariant.set("url", extendedVideoVariant.getUrl());
					listVideoVariants.add(builderVideoVariant.build());
				}
				builderVideoInfo.set("variants", listVideoVariants);
				builderExtendedMediaObject.set("video_info", builderVideoInfo.build());

				listExtendedMediaObject.add(builderExtendedMediaObject.build());
			}
			builderEntities.set("extended_entities", listExtendedMediaObject);
		} else
			builderEntities.set("extended_entities", Collections.emptyList());
		return builderEntities.build();
	}

	private static Record buildURLEntity(Schema schemaURLObject, URLEntity urlEntity) {
		GenericRecordBuilder builderURLObject = new GenericRecordBuilder(schemaURLObject);
		builderURLObject.set("url", urlEntity.getURL());
		builderURLObject.set("text", urlEntity.getText());
		builderURLObject.set("expanded_url", urlEntity.getExpandedURL());
		builderURLObject.set("display_url", urlEntity.getDisplayURL());
		builderURLObject.set("start", urlEntity.getStart());
		builderURLObject.set("end", urlEntity.getEnd());
		return builderURLObject.build();
	}

	private static Record buildPlace(Schema schemaPlace, Place place) {
		GenericRecordBuilder builderPlace = new GenericRecordBuilder(schemaPlace);
		builderPlace.set("country", place.getCountry());
		builderPlace.set("country_code", place.getCountryCode());
		builderPlace.set("full_name", place.getFullName());
		builderPlace.set("id", place.getId());
		builderPlace.set("name", place.getName());
		builderPlace.set("place_type", place.getPlaceType());
		builderPlace.set("bounding_box_type", place.getBoundingBoxType());
		builderPlace.set("geometry_type", place.getGeometryType());
		builderPlace.set("street_address", place.getStreetAddress());
		builderPlace.set("url", place.getURL());
		builderPlace.set("bounding_box", getPlaceCoordinates(place.getBoundingBoxCoordinates()));
		if (place.getGeometryCoordinates() != null)
			builderPlace.set("geometry", getPlaceCoordinates(place.getGeometryCoordinates()));
		/*
		 * if (place.getContainedWithIn() != null) { List<GenericRecord>
		 * listPlaceContainedWithin = new ArrayList<GenericRecord>(); for (Place
		 * place2 : place.getContainedWithIn()) {
		 * listPlaceContainedWithin.add(buildPlace(schemaPlace, place2)); }
		 * builderPlace.set("contained_within", listPlaceContainedWithin); }
		 */
		return builderPlace.build();
	}

	private static List<List<List<Double>>> getPlaceCoordinates(GeoLocation[][] geoLocations) {
		List<List<List<Double>>> listListListBoundingBox = new ArrayList<>();
		for (GeoLocation[] geoLocation1 : geoLocations) {
			List<List<Double>> listListBoundingBox = new ArrayList<>();
			for (GeoLocation geoLocation2 : geoLocation1) {
				List<Double> listBoundingBox = new ArrayList<>();
				listBoundingBox.add(geoLocation2.getLatitude());
				listBoundingBox.add(geoLocation2.getLongitude());
				listListBoundingBox.add(listBoundingBox);
			}
			listListListBoundingBox.add(listListBoundingBox);
		}
		return listListListBoundingBox;
	}
}
