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

import com.couchbase.client.core.cnc.Event;
import com.couchbase.client.core.cnc.SimpleEventBus;
import com.couchbase.client.core.cnc.events.endpoint.EndpointDisconnectDelayedEvent;
import com.couchbase.client.core.cnc.events.endpoint.EndpointDisconnectResumedEvent;
import com.couchbase.client.core.config.ProposedGlobalConfigContext;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.IgnoreWhen;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.core.util.CbCollections.listOf;
import static com.couchbase.client.java.QueryIntegrationTest.verySlowQueryStatement;
import static com.couchbase.client.java.manager.query.QueryIndexManagerIntegrationTest.DISABLE_QUERY_TESTS_FOR_CLUSTER;
import static com.couchbase.client.java.query.QueryOptions.queryOptions;
import static com.couchbase.client.test.Util.waitUntilCondition;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

@IgnoreWhen(missesCapabilities = {Capabilities.QUERY, Capabilities.CLUSTER_LEVEL_QUERY},
  clusterVersionEquals = DISABLE_QUERY_TESTS_FOR_CLUSTER)
class QueryDelayDisconnectIntegrationTest extends JavaIntegrationTest {
  private static final Logger log = LoggerFactory.getLogger(QueryDelayDisconnectIntegrationTest.class);

  @Test
  void inflightQueryIsAllowedToCompleteIfNodeLeavesCluster() throws Throwable {
    // This test relies on a query timeout expiring. Minimize typical execution time by
    // starting with short timeout and trying again with a longer timeout if necessary.
    try {
      testWithQueryTimeout(Duration.ofSeconds(4)); // long enough for laptop or GHA
    } catch (Throwable t) {
      log.warn("Test failed with short query timeout. Trying again with longer timeout.", t);
      testWithQueryTimeout(Duration.ofSeconds(40)); // long enough for a glacial CI environment
    }
  }

  static void testWithQueryTimeout(Duration queryTimeout) throws Exception {
    Duration gracePeriod = queryTimeout.dividedBy(3);
    log.info("Testing with queryTimeout={} gracePeriod={}", queryTimeout, gracePeriod);

    SimpleEventBus eventBus = new SimpleEventBus(true);

    try (Cluster cluster = createCluster(env -> env
      .eventBus(eventBus)
      .ioConfig(io -> io.configPollInterval(Duration.ofHours(24)))
    )) {
      String clientContextId = UUID.randomUUID().toString(); // so we can monitor query state
      CompletableFuture<Throwable> queryErrorFuture = new CompletableFuture<>();

      log.info("Scheduling a query we expect to time out.");
      cluster.reactive()
        .query(
          verySlowQueryStatement(),
          queryOptions()
            .clientContextId(clientContextId)
            .timeout(queryTimeout)
        )
        .subscribe(
          result -> queryErrorFuture.complete(new AssertionFailedError("Expected query to time out.")),
          queryErrorFuture::complete
        );

      log.info("Waiting for query execution to start.");
      waitUntilCondition(
        () -> assertEquals("running", getQueryState(cluster, clientContextId), "query state"),
        gracePeriod
      );

      log.info("Tricking the SDK into thinking all query nodes went away.");
      cluster.core().configurationProvider().proposeGlobalConfig(
        new ProposedGlobalConfigContext(dummyConfigWithNoQueryNodes, "127.0.0.1", true)
      );

      log.info("Verifying network channel closure was deferred.");
      waitUntilEvents(eventBus, gracePeriod, listOf(
        EndpointDisconnectDelayedEvent.class
      ));

      log.info("Waiting for query timeout.");
      Duration pollTimeout = queryTimeout.plus(gracePeriod);
      Throwable t = queryErrorFuture.get(pollTimeout.toMillis(), MILLISECONDS);
      assertInstanceOf(TimeoutException.class, t);

      log.info("Verifying network channel was closed.");
      waitUntilEvents(eventBus, gracePeriod, listOf(
        EndpointDisconnectDelayedEvent.class,
        EndpointDisconnectResumedEvent.class
      ));
    }
  }

  private static void waitUntilEvents(SimpleEventBus eventBus, Duration gracePeriod, List<Class<?>> expectedEventClasses) {
    waitUntilCondition(
      () -> assertEquals(expectedEventClasses, getEventClasses(eventBus)),
      gracePeriod
    );
  }

  private static List<Class<?>> getEventClasses(SimpleEventBus eventBus) {
    return eventBus.publishedEvents().stream()
      .filter(e -> e instanceof EndpointDisconnectDelayedEvent || e instanceof EndpointDisconnectResumedEvent)
      .map(Event::getClass)
      .collect(toList());
  }

  private static @Nullable String getQueryState(Cluster cluster, String clientContextId) {
    return cluster.query(
        "SELECT RAW state FROM system:active_requests WHERE clientContextID = ?",
        queryOptions()
          .parameters(JsonArray.from(clientContextId))
      )
      .rowsAs(String.class)
      .stream().findFirst().orElse(null);
  }

  private static final String dummyConfigWithNoQueryNodes = "{\n" +
    "  \"revEpoch\": 9999999999,\n" +
    "  \"rev\": 13205,\n" +
    "  \"nodesExt\": [\n" +
    "    {\n" +
    "      \"services\": {\n" +
    "        \"mgmt\": 8091,\n" +
    "        \"mgmtSSL\": 18091\n" +
    "      },\n" +
    "      \"thisNode\": true\n" +
    "    }\n" +
    "  ],\n" +
    "  \"clusterCapabilitiesVer\": [1, 0],\n" +
    "  \"clusterCapabilities\": {}\n" +
    "}\n";

}
