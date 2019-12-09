/*
 * Copyright (c) 2018 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"));
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

import com.couchbase.client.core.diag.HealthPinger;
import com.couchbase.client.core.diag.PingResult;
import com.couchbase.client.core.diag.PingServiceHealth;
import com.couchbase.client.core.endpoint.EndpointState;
import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import com.couchbase.client.test.Util;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static com.couchbase.client.test.Util.waitUntilCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class PingTest extends JavaIntegrationTest {

  private static Cluster cluster;
  private static Bucket bucket;

  @BeforeAll
  static void setup() {
    cluster = Cluster.connect(seedNodes(), clusterOptions());
    bucket = cluster.bucket(config().bucketname());

    // TODO: fix this hack once diagnostics and ping on all levels are stable
    waitUntilCondition(() -> cluster.core().clusterConfig().hasClusterOrBucketConfig());
  }

  @AfterAll
  static void tearDown() {
    cluster.disconnect();
  }

  @Test
  void ping() {
    PingResult pr = bucket.ping();
    assertFalse(pr.services().isEmpty());
    PingServiceHealth psh = pr.services().stream().filter(v -> v.type() == ServiceType.KV).findFirst().get();
    assertTrue(psh.latency() != 0);
    assertEquals(PingServiceHealth.PingState.OK, psh.state());
  }

  @IgnoreWhen(missesCapabilities = {Capabilities.QUERY})
  @Test
  void pingQuery() {
    PingServiceHealth psh = HealthPinger.pingQuery(config().bucketname(),
            cluster.core(),
            Duration.ofSeconds(2),
            FailFastRetryStrategy.INSTANCE).block();
    assertEquals(PingServiceHealth.PingState.OK, psh.state());
  }

  @IgnoreWhen(missesCapabilities = {Capabilities.SEARCH})
  @Test
  void pingSearch() {
    PingServiceHealth psh = HealthPinger.pingSearch(config().bucketname(),
            cluster.core(),
            Duration.ofSeconds(2),
            FailFastRetryStrategy.INSTANCE).block();
    assertEquals(PingServiceHealth.PingState.OK, psh.state());
  }

  @IgnoreWhen(missesCapabilities = {Capabilities.ANALYTICS})
  @Test
  void pingAnalytics() {
    PingServiceHealth psh = HealthPinger.pingAnalytics(config().bucketname(),
            cluster.core(),
            Duration.ofSeconds(2),
            FailFastRetryStrategy.INSTANCE).block();
    assertEquals(PingServiceHealth.PingState.OK, psh.state());
  }

  @Test
  void pingKV() {
    Util.waitUntilCondition(() -> {
      PingServiceHealth psh = HealthPinger.pingKV(config().bucketname(),
              cluster.core(),
              Duration.ofSeconds(2),
              FailFastRetryStrategy.INSTANCE).block();
      return PingServiceHealth.PingState.OK == psh.state();
    });
  }

  @IgnoreWhen(missesCapabilities = {Capabilities.VIEWS}, clusterTypes = ClusterType.MOCKED)
  @Test
  void pingViews() {
    PingServiceHealth psh = HealthPinger.pingViews(config().bucketname(),
            cluster.core(),
            Duration.ofSeconds(2),
            FailFastRetryStrategy.INSTANCE).block();
    assertEquals(PingServiceHealth.PingState.OK, psh.state());
  }
}


