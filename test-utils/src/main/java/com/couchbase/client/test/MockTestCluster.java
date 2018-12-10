/*
 * Copyright (c) 2018 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.test;

import com.couchbase.mock.Bucket;
import com.couchbase.mock.BucketConfiguration;
import com.couchbase.mock.CouchbaseMock;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Implements the integration test cluster on top of the CouchbaseMock library.
 *
 * @since 2.0.0
 */
public class MockTestCluster extends TestCluster {

  private static final int RANDOM_PORT = 0;

  private final Properties properties;

  private volatile CouchbaseMock mock;

  MockTestCluster(Properties properties) {
    this.properties = properties;
  }

  @Override
  TestClusterConfig _start() throws Exception {
    BucketConfiguration bucketConfig = new BucketConfiguration();
    bucketConfig.type = Bucket.BucketType.COUCHBASE;
    bucketConfig.numVBuckets = 1024;
    bucketConfig.numNodes = Integer.parseInt(properties.getProperty("cluster.mocked.numNodes"));
    bucketConfig.numReplicas = Integer.parseInt(properties.getProperty("cluster.mocked.numReplicas"));

    bucketConfig.name = UUID.randomUUID().toString();
    bucketConfig.password = UUID.randomUUID().toString();

    try {
      mock = new CouchbaseMock(RANDOM_PORT, Collections.singletonList(bucketConfig));
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }

    mock.start();
    mock.waitForStartup();

    Map<Services, Integer> ports = new HashMap<>();
    ports.put(Services.KV, mock.getCarrierPort(bucketConfig.name));

    return new TestClusterConfig(
      bucketConfig.name,
      bucketConfig.name,
      bucketConfig.password,
      Collections.singletonList(new TestNodeConfig(
        bucketConfig.hostname,
        ports
      ))
    );
  }

  @Override
  public void close() {
    if (mock != null) {
      mock.stop();
    }
  }
}
