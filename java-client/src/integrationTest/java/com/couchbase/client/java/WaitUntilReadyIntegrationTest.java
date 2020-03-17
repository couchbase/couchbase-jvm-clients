/*
 * Copyright (c) 2020 Couchbase, Inc.
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

import com.couchbase.client.core.cnc.EventBus;
import com.couchbase.client.core.cnc.SimpleEventBus;
import com.couchbase.client.core.error.UnambiguousTimeoutException;
import com.couchbase.client.java.env.ClusterEnvironment;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static com.couchbase.client.java.ClusterOptions.clusterOptions;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class WaitUntilReadyIntegrationTest {

  private static ClusterEnvironment environment;

  @BeforeAll
  static void beforeAll() {
    EventBus eventBus = new SimpleEventBus(true);
    environment = ClusterEnvironment.builder().eventBus(eventBus).build();
  }

  @AfterAll
  static void afterAll() {
    environment.shutdown();
  }

  @Test
  void timesOutClusterWhenNotReady() {
    Cluster cluster =  Cluster.connect(
      "127.0.0.1",
      clusterOptions("foo", "bar").environment(environment)
    );

    assertThrows(UnambiguousTimeoutException.class, () -> cluster.waitUntilReady(Duration.ofSeconds(2)));
    cluster.disconnect();
  }

  @Test
  void timesOutBucketWhenNotReady() {
    Cluster cluster =  Cluster.connect(
      "127.0.0.1",
      clusterOptions("foo", "bar").environment(environment)
    );
    Bucket bucket = cluster.bucket("foo");
    assertThrows(UnambiguousTimeoutException.class, () -> bucket.waitUntilReady(Duration.ofSeconds(2)));
    cluster.disconnect();
  }

}
