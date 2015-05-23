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
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Sets;
import com.sun.syndication.feed.synd.SyndEntry;
import com.sun.syndication.feed.synd.SyndFeed;
import com.sun.syndication.io.FeedException;
import com.sun.syndication.io.SyndFeedInput;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import samza.examples.rss.utils.Datum;
import samza.examples.rss.utils.FeedDetails;
import samza.examples.rss.utils.RFC3339Utils;
import samza.examples.rss.utils.SyndEntrySerializer;

public class RssFeed implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(RssFeed.class);

    private final String urlsFilePath;
    private final int timeOut;
    private final int waitTime;
    private List<FeedDetails> feedDetails;
    private SyndEntrySerializer serializer;
    private DateTime publishedSince;
    private RssConsumer rssConsumer;
    private boolean workFlag;

    private static final String RSS_KEY = "rssFeed";
    private static final String URI_KEY = "uri";
    private static final String LINK_KEY = "link";
    private static final String DATE_KEY = "publishedDate";

    protected static final Map<String, Set<String>> PREVIOUSLY_SEEN = new ConcurrentHashMap<>();

    /**
     * Constructor
     * @param urlsFilePath
     * @param timeOut
     * @param waitTime
     */
    public RssFeed(String urlsFilePath, int timeOut, int waitTime) {
        this.urlsFilePath = urlsFilePath;
        this.timeOut = timeOut;
        this.waitTime = waitTime;
        this.serializer = new SyndEntrySerializer();
        // TODO pass this as a parameter?
        this.publishedSince = new DateTime().withYear(2014)
                .withDayOfMonth(5).withMonthOfYear(9)
                .withZone(DateTimeZone.UTC);
    }

    /**
     * Starts the feed reader
     * @param rssConsumer
     */
    public void start(RssConsumer rssConsumer) {
        // load urls to be read
        feedDetails = readUrlFile();
        this.setConsumer(rssConsumer);
        this.workFlag = true;
    }

    public void run() {
        // start polling from urls
        checkForNewEntries();
    }

    private void checkForNewEntries() {
        while(workFlag) {
            this.rssConsumer.onIncommingBatch(getNextBatch());
        }
    }

    /**
     * Reads assigned URLs file from classpath
     *
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

    /**
     * Stops the feed reader
     */
    public void stop() {
        this.workFlag = false;
        log.info("Stoping RssFeed consumer.");
    }

    /**
     * Gets the next batch of feeds from URLs
     *
     * @return
     */
    public List<Datum> getNextBatch() {
        FeedDetails feedDetail = null;
        Set<String> batch = null;
        Iterator<FeedDetails> it = feedDetails.iterator();
        List<Datum> dataQueue = new ArrayList<>();
        while (it.hasNext()) {
            try {
                feedDetail = it.next();
                // if enough time has passed, then read again
                long elapsedTime = (System.currentTimeMillis() - feedDetail.getLastPolled())/1000;
                if (elapsedTime > feedDetail.getPollIntervalMillis()*60) {
                    log.info(feedDetail.getUrl() + " polling.");
                    // logging previously seen feeds
                    batch = queueFeedEntries(feedDetail, dataQueue);
                    PREVIOUSLY_SEEN.put(feedDetail.getUrl(), batch);
                    // updating previously seen feeds
                    feedDetail.setLastPolled(System.currentTimeMillis());
                } else {
                    log.info(feedDetail.getUrl() + " has been already polled.");
                    this.waiting(this.waitTime);
                }
            } catch (IOException e) {
                log.error("Error while reading data from RSS. IOException. " + feedDetail.getUrl());
                e.printStackTrace();
            } catch (FeedException e) {
                log.error("Error while reading data from RSS. FeedException. " + feedDetail.getUrl());
                e.printStackTrace();
            } catch (InterruptedException e) {
                log.error("Error while reading data from RSS. InterruptedException." + feedDetail);
                e.printStackTrace();
            }
        }
        return dataQueue;
    }

    /**
     * Reads the url and queues the data
     *
     * @param feedDetail feedDetails object
     * @return set of all article urls that were read from the feed
     * @throws IOException                          when it cannot connect to the url or the url is malformed
     * @throws com.sun.syndication.io.FeedException when it cannot reed the feed.
     */
    protected Set<String> queueFeedEntries(FeedDetails feedDetail, List<Datum> dataQueue) throws IOException,
            FeedException {
        URL feedUrl = new URL(feedDetail.getUrl());
        URLConnection connection = feedUrl.openConnection();
        connection.setConnectTimeout(this.timeOut);
        SyndFeedInput input = new SyndFeedInput();
        SyndFeed feed = input.build(new InputStreamReader(connection.getInputStream()));
        Set<String> batch = Sets.newConcurrentHashSet();
        for (Object entryObj : feed.getEntries()) {
            SyndEntry entry = (SyndEntry) entryObj;
            ObjectNode nodeEntry = this.serializer.deserialize(entry);
            nodeEntry.put(RSS_KEY, feedDetail.getUrl());
            String entryId = determineId(nodeEntry);
            batch.add(entryId);
            Datum datum = new Datum(nodeEntry, entryId, DateTime.now());
            JsonNode published = nodeEntry.get(DATE_KEY);
            if (published != null) {
                try {
                    DateTime date = RFC3339Utils.parseToUTC(published.asText());
                    if (date.isAfter(this.publishedSince)
                            && (!seenBefore(entryId, feedDetail.getUrl()))) {
                        dataQueue.add(datum);
                        log.debug("Added entry, {}, to provider queue.", entryId);
                    }
                } catch (Exception e) {
                    log.trace("Failed to parse date from object node, attempting to add node to queue by default.");
                    if (!seenBefore(entryId, feedDetail.getUrl())) {
                        dataQueue.add(datum);
                        log.debug("Added entry, {}, to provider queue.", entryId);
                    }
                }
            } else {
                log.debug("No published date present, attempting to add node to queue by default.");
                if (!seenBefore(entryId, feedDetail.getUrl())) {
                    dataQueue.add(datum);
                    log.debug("Added entry, {}, to provider queue.",
                            entryId);
                }
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

    /**
     * Registers a new rssConsumer to whom entries can be delivered.
     * @param rssConsumer
     */
    public void setConsumer(RssConsumer rssConsumer) {
        this.rssConsumer = rssConsumer;
    }

    public static void main(String[] args) throws InterruptedException {
        RssFeed feed = new RssFeed("/Users/renatomarroquin/Documents/workspace/workspaceApache/hello-samza/src/main/resources/rss.file", 3000, 10000);
        feed.start(new RssConsumer("rss", feed, null));

        feed.checkForNewEntries();

        Thread.sleep(20000);
        feed.stop();
    }
}