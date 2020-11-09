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

package com.couchbase.client.metrics.micrometer;

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.test.ClusterAwareIntegrationTest;
import com.couchbase.client.test.Services;
import com.couchbase.client.test.TestNodeConfig;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static com.couchbase.client.java.ClusterOptions.clusterOptions;
import static com.couchbase.client.test.Util.waitUntilCondition;

class MicrometerMeterIntegrationTest extends ClusterAwareIntegrationTest {

  private static ClusterEnvironment environment;
  private static Cluster cluster;
  private static Collection collection;

  private static MeterRegistry meterRegistry;

  @BeforeAll
  static void beforeAll() {
    TestNodeConfig config = config().firstNodeWith(Services.KV).get();

    meterRegistry = new SimpleMeterRegistry();

    environment = ClusterEnvironment
      .builder()
      .meter(MicrometerMeter.wrap(meterRegistry))
      .build();

    cluster = Cluster.connect(
      String.format("couchbase://%s:%d", config.hostname(), config.ports().get(Services.KV)),
      clusterOptions(config().adminUsername(), config().adminPassword())
        .environment(environment)
    );
    Bucket bucket = cluster.bucket(config().bucketname());
    collection = bucket.defaultCollection();

    bucket.waitUntilReady(Duration.ofSeconds(30));
  }

  @AfterAll
  static void afterAll() {
    cluster.disconnect();
    environment.shutdown();
    meterRegistry.close();
  }

  @Test
  void capturesMetrics() {
    int numRequests = 100;
    for (int i = 0; i < numRequests; i++) {
      try {
        collection.get("foo-" + i);
      } catch (DocumentNotFoundException x) {
        // expected
      }
    }

    waitUntilCondition(() -> {
      boolean counterGood = false;
      boolean summaryGood = false;
      for (io.micrometer.core.instrument.Meter meter : meterRegistry.getMeters()) {
        if (meter instanceof Counter) {
          counterGood = ((Counter) meter).count() >= numRequests
            && meter.getId().getName().equals("cb.requests");
        } else if (meter instanceof DistributionSummary) {
          summaryGood = ((DistributionSummary) meter).count() >= numRequests
            && meter.getId().getName().equals("cb.responses");
        }
      }
      return counterGood && summaryGood;
    });
  }

}
