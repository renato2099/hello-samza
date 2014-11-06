#!/bin/bash -e
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# This script will download, setup, start, and stop servers for Kafka, YARN, and ZooKeeper,
# as well as downloading, building and locally publishing Samza

./bin/grid stop all
mvn clean package
./bin/grid bootstrap
mkdir deploy/samza
tar -xvf samza-job-package/target/samza-job-package-0.7.0-dist.tar.gz -C deploy/samza
deploy/samza/bin/run-job.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file:/Users/renatomarroquin/Documents/workspace/workspaceApache/hello-samza/deploy/samza/config/order-feed.properties
./deploy/kafka/bin/kafka-console-consumer.sh  --zookeeper localhost:2181 --topic order-raw --from-beginning
