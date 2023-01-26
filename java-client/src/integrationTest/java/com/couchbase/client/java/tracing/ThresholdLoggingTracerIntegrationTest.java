/*
 * Copyright (c) 2021 Couchbase, Inc.
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

package com.couchbase.client.java.tracing;

import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.core.cnc.SimpleEventBus;
import com.couchbase.client.core.cnc.events.tracing.OverThresholdRequestsRecordedEvent;
import com.couchbase.client.core.env.ThresholdLoggingTracerConfig;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.couchbase.client.test.Util.waitUntilCondition;
import static org.junit.jupiter.api.Assertions.assertTrue;

@IgnoreWhen(isProtostellarWillWorkLater = true)
public class ThresholdLoggingTracerIntegrationTest extends JavaIntegrationTest {

  static private Cluster cluster;
  static private Collection collection;
  static private SimpleEventBus eventBus;

  @BeforeAll
  static void beforeAll() {
    eventBus = new SimpleEventBus(false);

    ThresholdLoggingTracerConfig.Builder config = ThresholdLoggingTracerConfig
      .enabled(true)
      .kvThreshold(Duration.ofNanos(1))
      .queryThreshold(Duration.ofNanos(1))
      .emitInterval(Duration.ofSeconds(2));

    cluster = createCluster(env -> env
        .thresholdLoggingTracerConfig(config)
        .eventBus(eventBus));

    Bucket bucket = cluster.bucket(config().bucketname());
    collection = bucket.defaultCollection();

    bucket.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);
  }

  @AfterEach
  void afterEach() {
    eventBus.clear();
  }

  @AfterAll
  static void afterAll() {
    cluster.disconnect();
  }


  @Test
  void logsAboveKvThreshold() {
    String docId = UUID.randomUUID().toString();
    collection.upsert(docId, JsonObject.create());
    collection.get(docId);

    assertInEvent("get");
    assertInEvent("upsert");
  }

  @Test
  @IgnoreWhen(missesCapabilities = Capabilities.QUERY, clusterTypes = ClusterType.CAVES)
  void logsAboveQueryThreshold() {
    cluster.query("select 1=1");
    assertInEvent("query");
  }

  private void assertInEvent(String opName) {
    waitUntilCondition(() ->
      eventBus.publishedEvents().stream().anyMatch(e -> e instanceof OverThresholdRequestsRecordedEvent)
    );

    List<Event> events = eventBus
      .publishedEvents()
      .stream()
      .filter(e -> e instanceof OverThresholdRequestsRecordedEvent)
      .collect(Collectors.toList());

    boolean found = false;
    for (Event event : events) {
      String output = event.description();
      if (output.contains("\"operation_name\":\"" + opName + "\"")) {
        found = true;
      }
    }

    assertTrue(found, "Operation Name: " + opName + " not found!");
  }

}
