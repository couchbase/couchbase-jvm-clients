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

package com.couchbase.client.tracing.opentelemetry;

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.test.ClusterAwareIntegrationTest;
import com.couchbase.client.test.Services;
import com.couchbase.client.test.TestNodeConfig;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import java.time.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static com.couchbase.client.java.ClusterOptions.clusterOptions;
import static com.couchbase.client.test.Util.waitUntilCondition;
import static io.opentelemetry.sdk.testing.assertj.OpenTelemetryAssertions.assertThat;

class OpenTelemetryIntegrationTest extends ClusterAwareIntegrationTest {

  private static ClusterEnvironment environment;
  private static Cluster cluster;
  private static Collection collection;

  @RegisterExtension
  static final OpenTelemetryExtension otelTesting = OpenTelemetryExtension.create();

  @BeforeAll
  static void beforeAll() {
    TestNodeConfig config = config().firstNodeWith(Services.KV).get();

    environment = ClusterEnvironment
      .builder()
      .requestTracer(OpenTelemetryRequestTracer.wrap(otelTesting.getOpenTelemetry()))
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
  }

  @Test
  void capturesTraceSpans() {
    Span parent = otelTesting.getOpenTelemetry().getTracer("integrationtest")
        .spanBuilder("test")
        .setSpanKind(SpanKind.SERVER)
        .startSpan();
    try (Scope ignored = parent.makeCurrent()) {
      collection.get("myid");
    } catch (DocumentNotFoundException ignored) {
      // expected
    } finally {
      parent.end();
    }

    waitUntilCondition(() -> {
      otelTesting.assertTraces()
          .hasTracesSatisfyingExactly(
              trace -> trace.hasSpansSatisfyingExactly(
                  span -> span
                      .hasName("test")
                      .hasKind(SpanKind.SERVER),
                  span -> span
                      .hasName("get")
                      .hasParentSpanId(parent.getSpanContext().getSpanId())
                      .hasKind(SpanKind.INTERNAL)
                      .hasAttributesSatisfying(attributes -> assertThat(attributes)
                          .containsEntry("db.system", "couchbase")
                          .containsEntry("db.operation", "get")
                          .containsEntry("db.couchbase.service", "kv")
                          .containsEntry("db.couchbase.collection", "_default")
                          .containsEntry("db.couchbase.scope", "_default")),
                  span -> span
                      .hasName("dispatch_to_server")
                      .hasKind(SpanKind.INTERNAL)
                      .hasAttributesSatisfying(attributes -> assertThat(attributes)
                          .containsEntry("db.system", "couchbase"))
          ));
      return true;
    });
  }

  @Test
  void stressTest() {
    int numRequests = 100;
    for (int i = 0; i < 100; i++) {
      try {
        collection.get("foo-" + i);
      } catch (DocumentNotFoundException x) {
        // expected
      }
    }

    waitUntilCondition(() -> {
      otelTesting.assertTraces().hasSizeGreaterThanOrEqualTo(numRequests);
      return true;
    });
  }

}
