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

package com.couchbase.client.tracing.opentracing;

import com.couchbase.client.core.env.SeedNode;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.test.ClusterAwareIntegrationTest;
import com.couchbase.client.test.Services;
import com.couchbase.client.test.TestNodeConfig;
import io.opentracing.mock.MockTracer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static com.couchbase.client.java.ClusterOptions.clusterOptions;
import static com.couchbase.client.test.Util.waitUntilCondition;

class OpenTracingIntegrationTest extends ClusterAwareIntegrationTest {

  private static ClusterEnvironment environment;
  private static Cluster cluster;
  private static Collection collection;
  private static MockTracer tracer;

  @BeforeAll
  static void beforeAll() {
    tracer = new MockTracer();

    TestNodeConfig config = config().firstNodeWith(Services.KV).get();

    environment = ClusterEnvironment.builder().requestTracer(OpenTracingRequestTracer.wrap(tracer)).build();

    Set<SeedNode> seedNodes = new HashSet<>(Collections.singletonList(
      SeedNode.create(config.hostname(), Optional.of(config.ports().get(Services.KV)), Optional.empty()))
    );
    cluster = Cluster.connect(
      seedNodes,
      clusterOptions(config().adminUsername(), config().adminPassword())
        .environment(environment)
    );
    Bucket bucket = cluster.bucket(config().bucketname());
    collection = bucket.defaultCollection();

    bucket.waitUntilReady(Duration.ofSeconds(15));
  }

  @AfterAll
  static void afterAll() {
    cluster.disconnect();
    environment.shutdown();
  }

  @Test
  void capturesTraceSpans() {
    int numRequests = 100;
    for (int i = 0; i < 100; i++) {
      try {
        collection.get("foo-" + i);
      } catch (DocumentNotFoundException x) {
        // expected
      }
    }

    waitUntilCondition(() -> tracer.finishedSpans().size() >= numRequests);
  }

}
