/*
 * Copyright (c) 2019 Couchbase, Inc.
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

package com.couchbase.client.java;

import com.couchbase.client.core.env.ServiceConfig;
import com.couchbase.client.core.service.KeyValueServiceConfig;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.util.JavaIntegrationTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KeyValueManyEndpointsTest extends JavaIntegrationTest {

  private Cluster cluster;
  private ClusterEnvironment environment;
  private Collection collection;

  @BeforeEach
  void beforeEach() {
    ServiceConfig.Builder serviceConfig = ServiceConfig.keyValueServiceConfig(KeyValueServiceConfig.builder().endpoints(8));
    environment = environment().serviceConfig(serviceConfig).build();
    cluster = Cluster.connect(connectionString(), ClusterOptions.clusterOptions(authenticator()).environment(environment));
    Bucket bucket = cluster.bucket(config().bucketname());
    collection = bucket.defaultCollection();
  }

  @AfterEach
  void afterEach() {
    cluster.shutdown();
    environment.shutdown();
  }

  /**
   * This test makes sure that even when many kv endpoints per node are opened, all kinds of request go through
   * since they are distributed across them.
   *
   * <p>Since the index is built by looking at the partition of the key, we iterate through many IDs to catch any
   * out of bounds problems.</p>
   */
  @Test
  void usesAllEndpointsPerNode() {
    for (int i = 0; i < 2048; i++) {
      collection.upsert("id-" + i, "foobar");
      assertEquals("foobar", collection.get("id-" + i).contentAs(String.class));
    }
  }

}
