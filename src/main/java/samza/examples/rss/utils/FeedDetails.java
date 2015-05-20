/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
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

package samza.examples.rss.utils;

public class FeedDetails {

    private String url;
    private Integer pollIntervalMillis;
    private Long lastPolled;

    public FeedDetails(String string, String pollInterval) {
        this.setUrl(string);
        this.setPollIntervalMillis(Integer.parseInt(pollInterval));
        this.setLastPolled(0L);
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Integer getPollIntervalMillis() {
        return pollIntervalMillis;
    }

    public void setPollIntervalMillis(Integer pollIntervalMillis) {
        this.pollIntervalMillis = pollIntervalMillis;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        return sb.append("[").append(url).append(":")
                .append(pollIntervalMillis).append("]").toString();
    }

    public Long getLastPolled() {
        return lastPolled;
    }

    public void setLastPolled(Long lastPolled) {
        this.lastPolled = lastPolled;
    }
}
