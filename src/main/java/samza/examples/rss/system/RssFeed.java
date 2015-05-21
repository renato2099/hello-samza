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

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.prefs.Preferences;

import org.apache.samza.SamzaException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import samza.examples.rss.utils.FeedDetails;

public class RssFeed {
    private static final Logger log = LoggerFactory.getLogger(RssFeed.class);
    private static final ObjectMapper jsonMapper = new ObjectMapper();

    private Connection con = null;
    private Statement st = null;
    private ResultSet rs = null;
    private final String urlsFilePath;

    public RssFeed(String urlsFilePath) {
        this.urlsFilePath = urlsFilePath;
    }

    public void start() {
        BlockingQueue<FeedDetails> feedDetails = readUrlFile();
        while(!feedDetails.isEmpty()) {
            System.out.println(feedDetails.poll());
        }
//        String url = "jdbc:mysql://" + host + ":" + String.valueOf(port) + "/"
//                + database;
//        try {
//            Class.forName("com.mysql.jdbc.Driver");
//            con = DriverManager.getConnection(url, user, password);
//            st = con.createStatement();
//            rs = st.executeQuery("SELECT * FROM `order`");
//        } catch (SQLException e) {
//            log.error(e.getMessage());
//            e.printStackTrace();
//        } catch (ClassNotFoundException e) {
//            log.error("Error while starting.");
//            e.printStackTrace();
//        }
    }

    private BlockingQueue<FeedDetails> readUrlFile() {
        BlockingQueue<FeedDetails> rssQueue = new LinkedBlockingQueue<FeedDetails>();
        try {
            List<String> readAllLines = Files.readAllLines(FileSystems
                    .getDefault().getPath(this.urlsFilePath), Charset.defaultCharset());
            for (String line : readAllLines) {
                if (!line.startsWith("#")) {
                    String[] split = line.split(",");
                    rssQueue.put(new FeedDetails(split[0], split[1]));
                }
            }
        } catch (IOException e) {
            log.error("Error while reading RSS list file.");
            e.printStackTrace();
        } catch (InterruptedException e) {
            log.error("Error while reading RSS list file.");
            e.printStackTrace();
        }
        return rssQueue;
    }

    public void stop() {
        try {
            if (rs != null) {
                rs.close();
            }
            if (st != null) {
                st.close();
            }
            if (con != null) {
                con.close();
            }

        } catch (SQLException ex) {
            log.error(ex.getMessage());
            ex.printStackTrace();
        }
    }

    public OrdersFeedRow getNext() {
        OrdersFeedRow or = null;
        try {
            if (rs.next()) {
                or = new OrdersFeedRow(rs.getLong(1), rs.getLong(2),
                        rs.getLong(3), rs.getDate(4), rs.getLong(5));
            }
        } catch (SQLException e) {
            log.error("Error in getNext.");
            e.printStackTrace();
        }
        return or;
    }

    public static final class OrdersFeedRow {

        private final String tableName = "order";
        private final long orderId;
        private final long customerId;
        private final long branchId;
        private final Date orderDate;
        private final long status;

        public OrdersFeedRow(long orderId, long customerId, long branchId,
                            Date orderDate, long status) {
            super();
            this.orderId = orderId;
            this.customerId = customerId;
            this.branchId = branchId;
            this.orderDate = orderDate;
            this.status = status;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + (int) (branchId ^ (branchId >>> 32));
            result = prime * result + (int) (customerId ^ (customerId >>> 32));
            result = prime * result
                    + ((orderDate == null) ? 0 : orderDate.hashCode());
            result = prime * result + (int) (orderId ^ (orderId >>> 32));
            result = prime * result + (int) (status ^ (status >>> 32));
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            OrdersFeedRow other = (OrdersFeedRow) obj;
            if (branchId != other.branchId) {
                return false;
            }
            if (customerId != other.customerId) {
                return false;
            }
            if (orderDate == null) {
                if (other.orderDate != null) {
                    return false;
                }
            } else if (!orderDate.equals(other.orderDate)) {
                return false;
            }
            if (orderId != other.orderId) {
                return false;
            }
            if (status != other.status) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return "OrderFeedEvent [order_id=" + orderId + ", customer_id="
                    + customerId + "," + " branch_id=" + branchId
                    + ", order_date=" + orderDate + ", status=" + status + "]";
        }

        public String toJson() {
            return toJson(this);
        }

        public static Map<String, Object> toMap(OrdersFeedRow event) {
            Map<String, Object> jsonObject = new HashMap<String, Object>();

            jsonObject.put("order_id", event.getOrderId());
            jsonObject.put("customer_id", event.getCustomerId());
            jsonObject.put("branch_id", event.getBranchId());
            jsonObject.put("order_date", event.getOrderDate());
            jsonObject.put("status", event.getStatus());

            return jsonObject;
        }

        public static String toJson(OrdersFeedRow event) {
            Map<String, Object> jsonObject = toMap(event);

            try {
                return jsonMapper.writeValueAsString(jsonObject);
            } catch (Exception e) {
                throw new SamzaException(e);
            }
        }

        public long getOrderId() {
            return orderId;
        }

        public long getCustomerId() {
            return customerId;
        }

        public long getBranchId() {
            return branchId;
        }

        public Date getOrderDate() {
            return orderDate;
        }

        public long getStatus() {
            return status;
        }

        public String getTableName() {
            return tableName;
        }
    }

    public static void main(String[] args) throws InterruptedException {
//
        RssFeed feed = new RssFeed("/Users/renatomarroquin/Documents/workspace/workspaceApache/hello-samza/src/main/resources/rss.file");
        feed.start();
//        feed.start();
//        OrdersFeedRow or = feed.getNext();
//        while(or != null) {
//            System.out.println(or.toJson());
//            or = feed.getNext();
//        }
//        feed.stop();
    }
}