/*
 * Copyright (c) 2023 Couchbase, Inc.
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

import com.couchbase.client.core.error.AmbiguousTimeoutException;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;

@IgnoreWhen(missesCapabilities = {Capabilities.PROTOSTELLAR})
class ClusterConnectionProtostellarIntegrationTest extends JavaIntegrationTest {

  @Test
  void canConnectWithProtostellarScheme() {
    String connectionString = "protostellar://" + config().nodes().get(0).hostname();
    try (Cluster cluster1 = Cluster.connect(connectionString, config().adminUsername(), config().adminPassword())) {

      Collection collection = cluster1.bucket(config().bucketname()).defaultCollection();

      collection.insert(UUID.randomUUID().toString(), JsonObject.create());
    }
  }

  @Test
  void canConnectWithProtostellarSchemeAndPort() {
    String connectionString = "protostellar://" + config().nodes().get(0).hostname() + ":18098";
    try (Cluster cluster1 = Cluster.connect(connectionString, config().adminUsername(), config().adminPassword())) {

      Collection collection = cluster1.bucket(config().bucketname()).defaultCollection();

      collection.insert(UUID.randomUUID().toString(), JsonObject.create());
    }
  }

  @Test
  void cannotConnectWithProtostellarSchemeAndIncorrectPort() {
    String connectionString = "protostellar://" + config().nodes().get(0).hostname() + ":12345";
    try (Cluster cluster1 = Cluster.connect(connectionString, config().adminUsername(), config().adminPassword())) {

      Collection collection = cluster1.bucket(config().bucketname()).defaultCollection();

      assertThrows(AmbiguousTimeoutException.class, () ->
        collection.insert(UUID.randomUUID().toString(), JsonObject.create(), InsertOptions.insertOptions().timeout(Duration.ofMillis(100))));
    }
  }
}
