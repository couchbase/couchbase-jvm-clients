/*
 * Copyright (c) 2022 Couchbase, Inc.
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
package com.couchbase.client.metrics.opentelemetry;

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.test.ClusterAwareIntegrationTest;
import com.couchbase.client.test.Services;
import com.couchbase.client.test.TestNodeConfig;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;

import static com.couchbase.client.java.ClusterOptions.clusterOptions;
import static com.couchbase.client.test.Util.waitUntilCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OpenTelemetryMetricsIntegrationTest extends ClusterAwareIntegrationTest {

  private static final InMemoryMetricReader reader = InMemoryMetricReader.create();
  private static final SdkMeterProvider sdkMeterProvider = SdkMeterProvider.builder().registerMetricReader(reader).build();

  private static Cluster cluster;
  private static Collection collection;

  @RegisterExtension
  static final OpenTelemetryExtension otelTesting = OpenTelemetryExtension.create();

  @BeforeAll
  static void beforeAll() {
    TestNodeConfig config = config().firstNodeWith(Services.KV).get();

    otelTesting.getOpenTelemetry().getMeterProvider();

    cluster = Cluster.connect(
      String.format("couchbase://%s:%d", config.hostname(), config.ports().get(Services.KV)),
      clusterOptions(config().adminUsername(), config().adminPassword())
        .environment(env -> env.meter(OpenTelemetryMeter.wrap(sdkMeterProvider)))
    );
    Bucket bucket = cluster.bucket(config().bucketname());
    collection = bucket.defaultCollection();

    bucket.waitUntilReady(Duration.ofSeconds(30));
  }

  @AfterAll
  static void afterAll() {
    cluster.disconnect();
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
      for (MetricData metricData : reader.collectAllMetrics()) {
        assertEquals("db.couchbase.operations", metricData.getName());
        for (HistogramPointData pd : metricData.getHistogramData().getPoints()) {
          if ("get".equals(pd.getAttributes().get(AttributeKey.stringKey("db.operation")))) {
            assertTrue(pd.getCount() >= numRequests);
          }
        }
      }
      return true;
    });
  }

}
