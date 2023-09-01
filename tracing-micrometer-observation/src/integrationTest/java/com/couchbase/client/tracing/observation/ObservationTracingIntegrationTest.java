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

package com.couchbase.client.tracing.observation;

import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.test.ClusterAwareIntegrationTest;
import com.couchbase.client.test.Services;
import com.couchbase.client.test.TestNodeConfig;
import io.micrometer.common.KeyValue;
import io.micrometer.common.KeyValues;
import io.micrometer.core.tck.MeterRegistryAssert;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.exporter.FinishedSpan;
import io.micrometer.tracing.test.SampleTestRunner;
import io.micrometer.tracing.test.simple.SpansAssert;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static com.couchbase.client.java.ClusterOptions.clusterOptions;
import static com.couchbase.client.test.Util.waitUntilCondition;

class ObservationTracingIntegrationTest extends ClusterAwareIntegrationTest {

  private static final Logger log = LoggerFactory.getLogger(ObservationTracingIntegrationTest.class);

  private static Cluster cluster;
  private static Collection collection;

  static void beforeEach(ObservationRegistry observationRegistry) {
    TestNodeConfig config = config().firstNodeWith(Services.KV).get();

    cluster = Cluster.connect("couchbase://" + config.hostname(),
      clusterOptions(config().adminUsername(), config().adminPassword())
        .environment(env -> env.requestTracer(ObservationRequestTracer.wrap(observationRegistry)))
    );
    Bucket bucket = cluster.bucket(config().bucketname());
    collection = bucket.defaultCollection();

    bucket.waitUntilReady(Duration.ofSeconds(30));
  }

  @AfterEach
  void afterAll() {
    if (cluster != null) {
      cluster.disconnect();
      cluster = null;
    }
  }


  @Nested
  class IntegrationTest extends SampleTestRunner {

    @Override
    @SuppressWarnings("unchecked")
    public SampleTestRunnerConsumer yourCode() throws Exception {
      return (bb, meterRegistry) -> {
        ObservationTracingIntegrationTest.beforeEach(getObservationRegistry());

        Span parent = bb.getTracer().currentSpan();
        log.info("Running the query");
        try {
          collection.get("myid");
        } catch (DocumentNotFoundException ignored) {
          // expected
        }

        log.info("Asserting results");
        waitUntilCondition(() -> {
          log.debug("Found following spans " + bb.getFinishedSpans());
          SpansAssert.assertThat(bb.getFinishedSpans())
              .hasASpanWithName("get", spanAssert -> {
                spanAssert
                  .hasKindEqualTo(Span.Kind.CLIENT)
                  .hasTag("db.system", "couchbase")
                  .hasTag("db.operation", "get")
                  .hasTag("db.couchbase.service", "kv")
                  .hasTag("db.couchbase.collection", "_default")
                  .hasTag("db.couchbase.scope", "_default")
                  .has(new Condition<FinishedSpan>() {
                    @Override
                    public boolean matches(FinishedSpan value) {
                      return value.getParentId().equals(parent.context().spanId());
                    }
                  });
              });
          log.info("Span <get> found");
          SpansAssert.assertThat(bb.getFinishedSpans())
            .hasASpanWithName("dispatch_to_server", spanAssert -> {
              spanAssert
                .hasKindEqualTo(Span.Kind.CLIENT)
                .hasTag("db.system", "couchbase");
            });
          log.info("Span <dispatch_to_server> found");
          MeterRegistryAssert.assertThat(meterRegistry)
            .hasMeterWithNameAndTags(TracingIdentifiers.METER_OPERATIONS, KeyValues.of(KeyValue.of("db.system", "couchbase")));
          log.info("Timer found");
        });
      };
    }
  }

  @Nested
  class StressTest extends SampleTestRunner {

    @Override
    public SampleTestRunnerConsumer yourCode() throws Exception {
      return (bb, meterRegistry) -> {

        ObservationTracingIntegrationTest.beforeEach(getObservationRegistry());

        int numRequests = 100;
        for (int i = 0; i < 100; i++) {
          try {
            collection.get("foo-" + i);
          } catch (DocumentNotFoundException x) {
            // expected
          }
        }

        waitUntilCondition(() -> {
          SpansAssert.then(bb.getFinishedSpans()).hasSizeGreaterThanOrEqualTo(numRequests);
          return true;
        });
      };
    }
  }

}
