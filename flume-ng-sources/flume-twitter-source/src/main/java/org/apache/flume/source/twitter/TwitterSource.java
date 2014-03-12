/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.source.twitter;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.MediaEntity;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.User;
import twitter4j.auth.AccessToken;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Demo Flume source that connects via Streaming API to the 1% sample twitter
 * firehose, continously downloads tweets, converts them to Avro format and
 * sends Avro events to a downstream Flume sink.
 *
 * Requires the consumer and access tokens and secrets of a Twitter developer
 * account
 */

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TwitterSource
    extends AbstractSource
    implements EventDrivenSource, Configurable, StatusListener {

  private TwitterStream twitterStream;
  private Schema avroSchema;

  private long docCount = 0;
  private long startTime = 0;
  private long exceptionCount = 0;
  private long totalTextIndexed = 0;
  private long skippedDocs = 0;
  private long batchEndTime = 0;
  private final List<Record> docs = new ArrayList<Record>();
  private final ByteArrayOutputStream serializationBuffer =
      new ByteArrayOutputStream();
  private DataFileWriter<GenericRecord> dataFileWriter;

  private int maxBatchSize = 1000;
  private int maxBatchDurationMillis = 1000;

  // Fri May 14 02:52:55 +0000 2010
  private SimpleDateFormat formatterTo =
      new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
  private DecimalFormat numFormatter = new DecimalFormat("###,###.###");

  private static int REPORT_INTERVAL = 100;
  private static int STATS_INTERVAL = REPORT_INTERVAL * 10;
  private static final Logger LOGGER =
      LoggerFactory.getLogger(TwitterSource.class);

  public TwitterSource() {
  }

  @Override
  public void configure(Context context) {
    String consumerKey = context.getString("consumerKey");
    String consumerSecret = context.getString("consumerSecret");
    String accessToken = context.getString("accessToken");
    String accessTokenSecret = context.getString("accessTokenSecret");

    LOGGER.info("Consumer Key:        '" + consumerKey + "'");
    LOGGER.info("Consumer Secret:     '" + consumerSecret + "'");
    LOGGER.info("Access Token:        '" + accessToken + "'");
    LOGGER.info("Access Token Secret: '" + accessTokenSecret + "'");

    twitterStream = new TwitterStreamFactory().getInstance();
    twitterStream.setOAuthConsumer(consumerKey, consumerSecret);
    twitterStream.setOAuthAccessToken(new AccessToken(accessToken,
                                                      accessTokenSecret));
    twitterStream.addListener(this);
    avroSchema = createAvroSchema();
    dataFileWriter = new DataFileWriter<GenericRecord>(
        new GenericDatumWriter<GenericRecord>(avroSchema));

    maxBatchSize = context.getInteger("maxBatchSize", maxBatchSize);
    maxBatchDurationMillis = context.getInteger("maxBatchDurationMillis",
                                                maxBatchDurationMillis);
  }

  @Override
  public synchronized void start() {
    LOGGER.info("Starting twitter source {} ...", this);
    docCount = 0;
    startTime = System.currentTimeMillis();
    exceptionCount = 0;
    totalTextIndexed = 0;
    skippedDocs = 0;
    batchEndTime = System.currentTimeMillis() + maxBatchDurationMillis;
    twitterStream.sample();
    //
    // twitterStream.firehose(0);
    LOGGER.info("Twitter source {} started.", getName());
    // This should happen at the end of the start method, since this will 
    // change the lifecycle status of the component to tell the Flume 
    // framework that this component has started. Doing this any earlier 
    // tells the framework that the component started successfully, even 
    // if the method actually fails later.
    super.start();
  }

  @Override
  public synchronized void stop() {
    LOGGER.info("Twitter source {} stopping...", getName());
    twitterStream.shutdown();
    super.stop();
    LOGGER.info("Twitter source {} stopped.", getName());
  }

  public void onStatus(Status status)  {
    Record doc = extractRecord(avroSchema, status);
    LOGGER.info("Extracted record is : "  + doc.toString());
    if (doc == null) {
      return; // skip
    }
    docs.add(doc);
    if (docs.size() >= maxBatchSize ||
        System.currentTimeMillis() >= batchEndTime) {
      batchEndTime = System.currentTimeMillis() + maxBatchDurationMillis;
      byte[] bytes;
      try {
        bytes = serializeToAvro(avroSchema, docs);
      } catch (IOException e) {
        LOGGER.error("Exception while serializing tweet", e);
        return; //skip
      }
      Event event = EventBuilder.withBody(bytes);
      getChannelProcessor().processEvent(event); // send event to the flume sink
      docs.clear();
    }
    docCount++;
    if ((docCount % REPORT_INTERVAL) == 0) {
      LOGGER.info(String.format("Processed %s docs",
                                numFormatter.format(docCount)));
    }
    if ((docCount % STATS_INTERVAL) == 0) {
      logStats();
    }
  }

  private Schema createAvroSchema() {
    Schema avroSchema = Schema.createRecord("Doc", "adoc", null, false);
    List<Field> fields = new ArrayList<Field>();
    fields.add(new Field("id", Schema.create(Type.LONG), null, null));
    fields.add(new Field("created_at", createOptional(Schema.create(Type.STRING)), null, null));
    fields.add(new Field("retweet_count", createOptional(Schema.create(Type.INT)), null, null));
    fields.add(new Field("retweeted", createOptional(Schema.create(Type.BOOLEAN)), null, null));
    fields.add(new Field("in_reply_to_user_id", createOptional(Schema.create(Type.LONG)),
        null, null));
    fields.add(new Field("in_reply_to_status_id", createOptional(Schema.create(Type.LONG)),
        null, null));
    fields.add(new Field("geolocation_latitude", createOptional(Schema.create(Type.DOUBLE)),
        null, null));
    fields.add(new Field("geolocation_longitude", createOptional(Schema.create(Type.DOUBLE)),
        null, null));
    fields.add(new Field("favorite_count", createOptional(Schema.create(Type.INT)),
        null, null));
    fields.add(new Field("is_possibly_sensitive", createOptional(Schema.create(Type.BOOLEAN)),
        null, null));
    fields.add(new Field("is_favorited", createOptional(Schema.create(Type.BOOLEAN)),
        null, null));
    fields.add(new Field("is_truncated", createOptional(Schema.create(Type.BOOLEAN)),
        null, null));

    fields.add(new Field("source", createOptional(Schema.create(Type.STRING)), null, null));
    fields.add(new Field("text", createOptional(Schema.create(Type.STRING)), null, null));
    fields.add(new Field("lang", createOptional(Schema.create(Type.STRING)), null, null));

    fields.add(new Field("media_url_https", createOptional(Schema.create(Type.STRING)), null, null));
    fields.add(new Field("expanded_url", createOptional(Schema.create(Type.STRING)), null, null));

    fields.add(new Field("user_id", createOptional(Schema.create(Type.LONG)), null, null));
    fields.add(new Field("user_friends_count", createOptional(Schema.create(Type.INT)), null, null));
    fields.add(new Field("user_statuses_count",createOptional(Schema.create(Type.INT)), null, null));
    fields.add(new Field("user_followers_count", createOptional(Schema.create(Type.INT)), null, null));
    fields.add(new Field("user_created_at", createOptional(Schema.create(Type.STRING)), null, null));
    fields.add(new Field("user_location", createOptional(Schema.create(Type.STRING)), null, null));
    fields.add(new Field("user_description", createOptional(Schema.create(Type.STRING)), null, null));
    fields.add(new Field("user_screen_name", createOptional(Schema.create(Type.STRING)), null, null));
    fields.add(new Field("user_name", createOptional(Schema.create(Type.STRING)), null, null));
    fields.add(new Field("user_screen_name", createOptional(Schema.create(Type.STRING)), null, null));
    fields.add(new Field("user_timezone", createOptional(Schema.create(Type.STRING)), null, null));

    fields.add(new Field("place_id", createOptional(Schema.create(Type.STRING)), null, null));
    fields.add(new Field("place_country", createOptional(Schema.create(Type.STRING)), null, null));
    fields.add(new Field("place_countrycode", createOptional(Schema.create(Type.STRING)), null, null));
    fields.add(new Field("place_name", createOptional(Schema.create(Type.STRING)), null, null));
    fields.add(new Field("place_placetype", createOptional(Schema.create(Type.STRING)), null, null));
    fields.add(new Field("place_streetadress", createOptional(Schema.create(Type.STRING)), null, null));

    Schema user_mention = Schema.createRecord("UserMentionEntity", "doc", null, false);
    List<Field> user_mention_fields = new ArrayList<Field>();
    user_mention_fields.add(new Field("text", createOptional(Schema.create(Type.STRING)), null, null));
    user_mention_fields.add(new Field("name", createOptional(Schema.create(Type.STRING)), null, null));
    user_mention_fields.add(new Field("screenName", createOptional(Schema.create(Type.STRING)), null, null));
    user_mention_fields.add(new Field("id", createOptional(Schema.create(Type.LONG)), null, null));
    user_mention_fields.add(new Field("start", createOptional(Schema.create(Type.INT)), null, null));
    user_mention_fields.add(new Field("end", createOptional(Schema.create(Type.INT)), null, null));
    user_mention.setFields(user_mention_fields);
    fields.add(new Field("user_mentions", createOptional(Schema.createArray(user_mention)), null, null));

    avroSchema.setFields(fields);
    return avroSchema;
  }

  private Record extractRecord(Schema avroSchema, Status status) {
    User user = status.getUser();
    Record doc = new Record(avroSchema);

    doc.put("id", status.getId());
    doc.put("created_at", formatterTo.format(status.getCreatedAt()));
    doc.put("retweet_count", status.getRetweetCount());
    doc.put("retweeted", status.isRetweet());
    doc.put("in_reply_to_user_id", status.getInReplyToUserId());
    doc.put("in_reply_to_status_id", status.getInReplyToStatusId());
    if (status.getGeoLocation() != null) {
      doc.put("geolocation_latitude", status.getGeoLocation().getLatitude());
      doc.put("geolocation_longitude", status.getGeoLocation().getLongitude());
    }
    doc.put("favorite_count", status.getFavoriteCount());
    doc.put("is_possibly_sensitive", status.isPossiblySensitive());
    doc.put("is_favorited", status.isFavorited());
    doc.put("is_truncated", status.isTruncated());

    addString(doc, "source", status.getSource());
    addString(doc, "text", status.getText());
    addString(doc, "lang", status.getLang());

    MediaEntity[] mediaEntities = status.getMediaEntities();
    if (mediaEntities.length > 0) {
      addString(doc, "media_url_https", mediaEntities[0].getMediaURLHttps());
      addString(doc, "expanded_url", mediaEntities[0].getExpandedURL());
    }

    doc.put("user_id", user.getId());
    doc.put("user_friends_count", user.getFriendsCount());
    doc.put("user_statuses_count", user.getStatusesCount());
    doc.put("user_followers_count", user.getFollowersCount());
    doc.put("user_created_at", formatterTo.format(user.getCreatedAt()));
    addString(doc, "user_location", user.getLocation());
    addString(doc, "user_description", user.getDescription());
    addString(doc, "user_screen_name", user.getScreenName());
    addString(doc, "user_name", user.getName());
    addString(doc, "user_timezone", user.getTimeZone());

    if (status.getPlace() != null) {
      addString(doc, "place_id", status.getPlace().getId());
      addString(doc, "place_country", status.getPlace().getCountry());
      addString(doc, "place_countrycode", status.getPlace().getCountryCode());
      addString(doc, "place_name", status.getPlace().getName());
      addString(doc, "place_placetype", status.getPlace().getPlaceType());
      addString(doc, "place_streetadress", status.getPlace().getStreetAddress());
    }

    if (status.getUserMentionEntities() != null && status.getUserMentionEntities().length > 0) {
      doc.put("user_mentions", status.getUserMentionEntities());
    }

    return doc;
  }

  private byte[] serializeToAvro(Schema avroSchema, List<Record> docList)
      throws IOException {
    serializationBuffer.reset();
    dataFileWriter.create(avroSchema, serializationBuffer);
    for (Record doc2 : docList) {
      dataFileWriter.append(doc2);
    }
    dataFileWriter.close();
    return serializationBuffer.toByteArray();
  }

  private Schema createOptional(Schema schema) {
    return Schema.createUnion(Arrays.asList(schema, Schema.create(Type.NULL)));
  }

  private void addString(Record doc, String avroField, String val) {
    if (val == null) {
      return;
    }
    doc.put(avroField, val);
    totalTextIndexed += val.length();
  }

  private void logStats() {
    double mbIndexed = totalTextIndexed / (1024 * 1024.0);
    long seconds = (System.currentTimeMillis() - startTime) / 1000;
    seconds = Math.max(seconds, 1);
    LOGGER.info(String.format("Total docs indexed: %s, total skipped docs: %s",
                numFormatter.format(docCount), numFormatter.format(skippedDocs)));
    LOGGER.info(String.format("    %s docs/second",
                numFormatter.format(docCount / seconds)));
    LOGGER.info(String.format("Run took %s seconds and processed:",
                numFormatter.format(seconds)));
    LOGGER.info(String.format("    %s MB/sec sent to index",
                numFormatter.format(((float) totalTextIndexed / (1024 * 1024)) / seconds)));
    LOGGER.info(String.format("    %s MB text sent to index",
                numFormatter.format(mbIndexed)));
    LOGGER.info(String.format("There were %s exceptions ignored: ",
                numFormatter.format(exceptionCount)));
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
