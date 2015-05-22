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

import org.apache.samza.Partition;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.BlockingEnvelopeMap;
import samza.examples.orders.system.OrdersFeed.OrdersFeedRow;
import samza.examples.rss.utils.Datum;

import java.util.Set;
import java.util.concurrent.BlockingQueue;

public class RssConsumer extends BlockingEnvelopeMap {
    private final String systemName;
    private final RssFeed feed;
    SystemStreamPartition systemStreamPartition;

    public RssConsumer(String systemName, RssFeed feed,
                         MetricsRegistry registry) {
        this.systemName = systemName;
        this.feed = feed;
    }

    @Override
    public void register(SystemStreamPartition systemStreamPartition,
                         String startingOffset) {
        this.systemStreamPartition= new SystemStreamPartition(systemName,
                "rss", new Partition(0));
        super.register(this.systemStreamPartition, startingOffset);
    }

    @Override
    public void start() {
        feed.start();
        try {
            //rss should start polling
            BlockingQueue<Datum> nextBatch = feed.getNextBatch();
            //// every entry on the polled queue should be send as a message
//            OrdersFeedRow row = (OrdersFeedRow) feed.getNext();
//            while (row != null) {
//                put(systemStreamPartition, new IncomingMessageEnvelope(
//                        systemStreamPartition, null, null, row));
//                row = (OrdersFeedRow) feed.getNext();
//            }
        } catch (Exception e) {
            System.err.println(e);
        }
    }

    @Override
    public void stop() {
        feed.stop();
    }
}