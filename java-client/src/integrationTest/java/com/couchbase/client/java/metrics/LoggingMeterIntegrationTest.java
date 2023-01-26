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

package com.couchbase.client.java.metrics;

import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.core.cnc.SimpleEventBus;
import com.couchbase.client.core.cnc.events.metrics.LatencyMetricsAggregatedEvent;
import com.couchbase.client.core.env.LoggingMeterConfig;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.couchbase.client.test.Util.waitUntilCondition;
import static org.junit.jupiter.api.Assertions.assertTrue;

@IgnoreWhen(isProtostellarWillWorkLater = true)
class LoggingMeterIntegrationTest extends JavaIntegrationTest {

  static private Cluster cluster;
  static private Collection collection;
  static private SimpleEventBus eventBus;

  @BeforeAll
  static void beforeAll() {
    eventBus = new SimpleEventBus(false);
    cluster = createCluster(env -> env
        .loggingMeterConfig(LoggingMeterConfig.enabled(true).emitInterval(Duration.ofSeconds(2)))
        .eventBus(eventBus));
    Bucket bucket = cluster.bucket(config().bucketname());
    collection = bucket.defaultCollection();

    bucket.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);
  }

  @AfterAll
  static void afterAll() {
    cluster.disconnect();
  }

  @Test
  void aggregateMetrics() {
    String id = UUID.randomUUID().toString();
    collection.upsert(id, JsonObject.create());
    collection.get(id);

    waitUntilCondition(() ->
      eventBus.publishedEvents().stream().anyMatch(e -> e instanceof LatencyMetricsAggregatedEvent)
    );

    List<Event> events = eventBus
      .publishedEvents()
      .stream()
      .filter(e -> e instanceof LatencyMetricsAggregatedEvent)
      .collect(Collectors.toList());

    for (Event ev : events) {
      String desc = ev.description();
      assertTrue(desc.contains("{\"emit_interval_s\":2}"), "Actual description: " + desc);
      assertTrue(desc.contains("kv"), "Actual description: " + desc);
    }
  }

}
