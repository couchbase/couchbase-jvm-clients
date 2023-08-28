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

import com.couchbase.client.core.cnc.SimpleEventBus;
import com.couchbase.client.core.cnc.events.endpoint.EndpointDisconnectDelayedEvent;
import com.couchbase.client.core.cnc.events.endpoint.EndpointDisconnectResumedEvent;
import com.couchbase.client.core.cnc.events.endpoint.EndpointStateChangedEvent;
import com.couchbase.client.core.config.ProposedGlobalConfigContext;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.query.QueryScanConsistency;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.Capabilities;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.couchbase.client.java.manager.query.QueryIndexManagerIntegrationTest.DISABLE_QUERY_TESTS_FOR_CLUSTER;
import static com.couchbase.client.java.query.QueryOptions.queryOptions;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies the delayed disconnect of an in-progress query Must be in package com.couchbase.client.core to override
 * Core.createConfigurationProvider()
 * <p>
 * Disabling against 5.5. See comment on QueryIndexManagerIntegrationTest for details.
 */
@IgnoreWhen(missesCapabilities = {Capabilities.QUERY, Capabilities.CLUSTER_LEVEL_QUERY},
  clusterVersionEquals = DISABLE_QUERY_TESTS_FOR_CLUSTER, clusterTypes = ClusterType.CAVES)
class QueryDelayDisconnectIntegrationTest extends JavaIntegrationTest {

  private static ClusterEnvironment initEnv;
  private static Cluster initCluster;

  private static ClusterEnvironment environment;
  private static Cluster cluster;

  private static SimpleEventBus eventBus = new SimpleEventBus(true, Collections.singletonList(EndpointStateChangedEvent.class));

  @BeforeAll
  static void setup() throws ExecutionException, InterruptedException {

    // create a cluster for initialization.
    // do not use the same cluster for testing as the initialization cluster is polluted by accessing the bucket.

    initEnv = ClusterEnvironment.builder().build();
    ClusterOptions initOpts = ClusterOptions.clusterOptions(authenticator()).environment(initEnv);
    initCluster = Cluster.connect(connectionString(), initOpts);//createCluster(env ->  ClusterEnvironment.builder().eventBus(eventBus).build());
    Bucket bucket = initCluster.bucket(config().bucketname());

    bucket.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);
    waitForService(bucket, ServiceType.QUERY);
    waitForQueryIndexerToHaveKeyspace(initCluster, config().bucketname());

    createPrimaryIndex(initCluster, config().bucketname());

    for (int i = 0; i < 100; i++) {
      initCluster.bucket(config().bucketname()).defaultCollection().upsert("" + i, "{}");
    }

    // create a cluster for testing

    environment = ClusterEnvironment.builder().eventBus(eventBus).ioConfig(io -> io.configPollInterval(Duration.ofHours(24))).build();
    ClusterOptions opts = ClusterOptions.clusterOptions(authenticator()).environment(environment);
    cluster = Cluster.connect(connectionString(), opts);

  }

  @BeforeEach
  void beforeEach() {
  }

  @AfterEach
  void afterEach() {
    eventBus.clear();
  }

  @AfterAll
  static void tearDown() {
    cluster.disconnect();
    environment.shutdown();

    for (int i = 0; i < 100; i++) {
      initCluster.bucket(config().bucketname()).defaultCollection().remove("" + i);
    }
    initCluster.disconnect();
    initEnv.shutdown();
  }

  @Test
  void simpleQueryClose() throws InterruptedException, ExecutionException {

    // Start a query.
    // When the first row is retrieved, modify the configuration by removing the n1ql nodes
    // The query endpoint will not be closed until the query completes

    AtomicBoolean first = new AtomicBoolean(true);
    cluster.reactive().query(
        "select * from `" + config().bucketname() + "` a  UNNEST(SELECT b.* FROM `" + config().bucketname()
          + "` b) AS c",
        queryOptions().metrics(true).scanConsistency(QueryScanConsistency.REQUEST_PLUS)
      ).block()
      .rowsAs(byte[].class).doOnNext(it -> {
        if (first.compareAndSet(true, false)) {
          cluster.reactive().core().configurationProvider().proposeGlobalConfig(
            new ProposedGlobalConfigContext(dummyConfig, "localhost", true)
          );
        }
      }).blockLast();
    cluster.reactive().core().shutdown().block(); // flush out events
    List<Class> events = new LinkedList<>();
    eventBus.publishedEvents().stream().filter(e -> e instanceof EndpointDisconnectDelayedEvent || e instanceof EndpointDisconnectResumedEvent).forEach(e -> events.add(e.getClass()));
    assertEquals(2, events.size());
    assertEquals(events.get(0), EndpointDisconnectDelayedEvent.class);
    assertEquals(events.get(1), EndpointDisconnectResumedEvent.class);

  }

  String dummyConfig = "{\n" +
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
