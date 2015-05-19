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

package samza.examples.orders.system;

import org.apache.samza.Partition;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.BlockingEnvelopeMap;
import samza.examples.orders.system.OrdersFeed.OrdersFeedRow;

public class OrdersConsumer extends BlockingEnvelopeMap {
    private final String systemName;
    private final OrdersFeed feed;
    SystemStreamPartition systemStreamPartition;

    public OrdersConsumer(String systemName, OrdersFeed feed,
                         MetricsRegistry registry) {
        this.systemName = systemName;
        this.feed = feed;
    }

    @Override
    public void register(SystemStreamPartition systemStreamPartition,
                         String startingOffset) {
        this.systemStreamPartition= new SystemStreamPartition(systemName,
                "order", new Partition(0));
        super.register(this.systemStreamPartition, startingOffset);
    }

    @Override
    public void start() {
        feed.start();
        try {
            OrdersFeedRow row = (OrdersFeedRow) feed.getNext();
            while (row != null) {
                put(systemStreamPartition, new IncomingMessageEnvelope(
                        systemStreamPartition, null, null, row));
                row = (OrdersFeedRow) feed.getNext();
            }
        } catch (Exception e) {
            System.err.println(e);
        }
    }

    @Override
    public void stop() {
        feed.stop();
    }
}