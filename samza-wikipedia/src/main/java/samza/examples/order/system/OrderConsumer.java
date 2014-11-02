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

package samza.examples.order.system;

import java.sql.Date;
import java.util.ArrayList;
import java.util.List;

import org.apache.samza.Partition;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.BlockingEnvelopeMap;

import samza.examples.order.system.OrderFeed.OrderFeedRow;

public class OrderConsumer extends BlockingEnvelopeMap {
    private final List<String> channels;
    private final String systemName;
    private final OrderFeed feed;

    public OrderConsumer(String systemName, OrderFeed feed,
            MetricsRegistry registry) {
        this.channels = new ArrayList<String>();
        this.systemName = systemName;
        this.feed = feed;
        createPartition();
    }

    private void createPartition() {
        SystemStreamPartition systemStreamPartition = new SystemStreamPartition(
                systemName, "order", new Partition(0));

        try {
            put(systemStreamPartition, new IncomingMessageEnvelope(
                    systemStreamPartition, null, null, new OrderFeedRow(10, 10, 10, new Date(10), 10)));
        } catch (Exception e) {
            System.err.println(e);
        }

    }

    @Override
    public void register(SystemStreamPartition systemStreamPartition,
            String startingOffset) {
        super.register(systemStreamPartition, startingOffset);

        channels.add(systemStreamPartition.getStream());
    }

    @Override
    public void start() {
        feed.start();
    }

    @Override
    public void stop() {
        feed.stop();
    }

}
