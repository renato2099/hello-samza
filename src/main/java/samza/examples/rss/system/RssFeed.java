/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package samza.examples.rss.system;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Sets;
import com.sun.syndication.feed.synd.SyndEntry;
import com.sun.syndication.feed.synd.SyndFeed;
import com.sun.syndication.io.FeedException;
import com.sun.syndication.io.SyndFeedInput;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import samza.examples.rss.utils.Datum;
import samza.examples.rss.utils.FeedDetails;
import samza.examples.rss.utils.RFC3339Utils;
import samza.examples.rss.utils.SyndEntrySerializer;

public class RssFeed {
    private static final Logger log = LoggerFactory.getLogger(RssFeed.class);
    private static final ObjectMapper jsonMapper = new ObjectMapper();

    private final String urlsFilePath;
    private final int timeOut;
    private final int waitTime;
    private List<FeedDetails> feedDetails;
    private BlockingQueue<Datum> dataQueue;
    private SyndEntrySerializer serializer;
    private DateTime publishedSince;

    private static final String RSS_KEY = "rssFeed";
    private static final String URI_KEY = "uri";
    private static final String LINK_KEY = "link";
    private static final String DATE_KEY = "publishedDate";

    protected static final Map<String, Set<String>> PREVIOUSLY_SEEN = new ConcurrentHashMap<>();

    public RssFeed(String urlsFilePath, int timeOut, int waittime) {
        this.urlsFilePath = urlsFilePath;
        this.timeOut = timeOut;
        this.waitTime = waittime;
        this.serializer = new SyndEntrySerializer();
        this.dataQueue = new LinkedBlockingQueue<>();
        // TODO pass this as a parameter?
        this.publishedSince = new DateTime().withYear(2014)
                .withDayOfMonth(5).withMonthOfYear(9)
                .withZone(DateTimeZone.UTC);
    }

    public void start() {
        feedDetails = readUrlFile();
    }

    /**
     * Reads assigned URLs file from classpath
     * @return
     */
    private List<FeedDetails> readUrlFile() {
        List<FeedDetails> rssQueue = new ArrayList<>();
        try {
            InputStream in = this.getClass().getClassLoader().getResourceAsStream(this.urlsFilePath);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.startsWith("#")) {
                    String[] split = line.split(",");
                    rssQueue.add(new FeedDetails(split[0], split[1]));
                }
            }
            reader.close();
        } catch (IOException e) {
            log.error("Error while reading RSS list file.");
            e.printStackTrace();
        }
        return rssQueue;
    }

    public void stop() {
        log.info("Stoping RssFeed consumer.");
    }

    /**
     * Gets the next batch of feeds from URLs
     * @return
     */
    public Set<String> getNextBatch() {
        FeedDetails feedDetail = null;
        Set<String> batch = Sets.newConcurrentHashSet();
        Iterator<FeedDetails> it = feedDetails.iterator();
        while(it.hasNext()) {
            try {
                feedDetail = it.next();
                // if enough time has passed, then read again
                long elapsedTime = (System.currentTimeMillis() - feedDetail.getLastPolled()) / 1000;
                if (elapsedTime > feedDetail.getPollIntervalMillis()) {
                    // logging previously seen feeds
                    queueFeedEntries(feedDetail, batch);
                    PREVIOUSLY_SEEN.put(feedDetail.getUrl(), batch);
                    // updating previously seen feeds
                    feedDetail.setLastPolled(System.currentTimeMillis());
                } else {
                    log.info(feedDetail.getUrl() + " has been already polled.");
                    this.waiting(this.waitTime);
                }
            } catch (IOException e) {
                log.error("Error while reading data from RSS. " + feedDetail);
                e.printStackTrace();
            } catch (FeedException e) {
                log.error("Error while reading data from RSS. " + feedDetail);
                e.printStackTrace();
            } catch (InterruptedException e) {
                log.error("Error while reading data from RSS. " + feedDetail);
                e.printStackTrace();
            }
        }
        return batch;
    }

    /**
     * Reads the url and queues the data
     *
     * @param feedDetail
     *            feedDetails object
     * @return set of all article urls that were read from the feed
     * @throws IOException
     *             when it cannot connect to the url or the url is malformed
     * @throws com.sun.syndication.io.FeedException
     *             when it cannot reed the feed.
     */
    protected Set<String> queueFeedEntries(FeedDetails feedDetail, Set<String> batch) throws IOException,
            FeedException {
        URL feedUrl = new URL(feedDetail.getUrl());
        URLConnection connection = feedUrl.openConnection();
        connection.setConnectTimeout(this.timeOut);
        connection.setConnectTimeout(this.timeOut);
        SyndFeedInput input = new SyndFeedInput();
        SyndFeed feed = input.build(new InputStreamReader(connection
                .getInputStream()));
        for (Object entryObj : feed.getEntries()) {
            SyndEntry entry = (SyndEntry) entryObj;
            ObjectNode nodeEntry = this.serializer.deserialize(entry);
            nodeEntry.put(RSS_KEY, feedDetail.getUrl());
            String entryId = determineId(nodeEntry);
            batch.add(entryId);
            Datum datum = new Datum(nodeEntry);
            try {
                JsonNode published = nodeEntry.get(DATE_KEY);
                if (published != null) {
                    try {
                        DateTime date = RFC3339Utils.parseToUTC(published.asText());
                        if (date.isAfter(this.publishedSince)
                                && (!seenBefore(entryId, feedDetail.getUrl()))) {
                            this.dataQueue.put(datum);
                            log.debug("Added entry, {}, to provider queue.", entryId);
                        }
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    } catch (Exception e) {
                        log.trace("Failed to parse date from object node, attempting to add node to queue by default.");
                        if (!seenBefore(entryId, feedDetail.getUrl())) {
                            this.dataQueue.put(datum);
                            log.debug("Added entry, {}, to provider queue.", entryId);
                        }
                    }
                } else {
                    log.debug("No published date present, attempting to add node to queue by default.");
                    if (!seenBefore(entryId, feedDetail.getUrl())) {
                        this.dataQueue.put(datum);
                        log.debug("Added entry, {}, to provider queue.",
                                entryId);
                    }
                }
            } catch (InterruptedException ie) {
                log.error("Interupted Exception.");
                Thread.currentThread().interrupt();
            }
        }
        return batch;
    }

    /**
     * Safe waiting
     *
     * @param waitTime
     * @throws InterruptedException
     */
    private void waiting(long waitTime) throws InterruptedException {
        log.warn("Waiting for " + waitTime + " mlsecs.");
        synchronized (this) {
            this.wait(waitTime);
        }
    }

    /**
     * Returns a link to the article to use as the id
     *
     * @param node
     * @return
     */
    private String determineId(ObjectNode node) {
        String id = null;
        if (node.get(URI_KEY) != null
                && !node.get(URI_KEY).textValue().equals("")) {
            id = node.get(URI_KEY).textValue();
        } else if (node.get(LINK_KEY) != null
                && !node.get(LINK_KEY).textValue().equals("")) {
            id = node.get(LINK_KEY).textValue();
        }
        return id;
    }

    /**
     * Returns false if the artile was previously seen in another task for this
     * feed
     *
     * @param id
     * @param rssFeed
     * @return
     */
    private boolean seenBefore(String id, String rssFeed) {
        Set<String> previousBatch = PREVIOUSLY_SEEN.get(rssFeed);
        if (previousBatch == null) {
            return false;
        }
        return previousBatch.contains(id);
    }
}