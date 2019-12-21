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

package com.couchbase.client.java.tracing;

import com.couchbase.client.core.cnc.InternalSpan;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.RequestTracer;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.util.JavaIntegrationTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class RequestTracerIntegrationTest extends JavaIntegrationTest {

  static private Cluster cluster;
  static private Collection collection;
  static private ClusterEnvironment environment;
  static private TrackingRequestTracer requestTracer;

  @BeforeAll
  static void beforeAll() {
    requestTracer = new TrackingRequestTracer();
    environment = environment().requestTracer(requestTracer).build();
    cluster = Cluster.connect(
      seedNodes(),
      ClusterOptions.clusterOptions(authenticator()).environment(environment)
    );
    Bucket bucket = cluster.bucket(config().bucketname());
    collection = bucket.defaultCollection();

    bucket.waitUntilReady(Duration.ofSeconds(5));
  }

  @AfterAll
  static void afterAll() {
    cluster.disconnect();
    environment.shutdown();
  }

  @Test
  void sendsStartAndEndRequestEventsOnSuccess() {
    String id = UUID.randomUUID().toString();
    collection.upsert(id, JsonObject.empty());
    collection.get(id);

    int totalFinished = 0;
    for (TrackingInternalSpan span : requestTracer.spans) {
      assertTrue(span.finished);
      totalFinished++;
    }
    assertTrue(totalFinished > 0);
  }

  @Test
  void sendsStartAndEndRequestEventsOnFailure() {
    String id = UUID.randomUUID().toString();
    collection.upsert(id, JsonObject.empty());
    try {
      collection.get(id, GetOptions.getOptions().timeout(Duration.ofNanos(1)));
      fail("Expected timeout exception");
    } catch (TimeoutException ex) {
      assertTrue(true);
    }

    int totalFinished = 0;
    for (TrackingInternalSpan span : requestTracer.spans) {
      assertTrue(span.finished);
      totalFinished++;
    }
    assertTrue(totalFinished > 0);
  }

  private static class TrackingRequestTracer implements RequestTracer {
    final List<TrackingInternalSpan> spans = Collections.synchronizedList(new ArrayList<>());

    @Override
    public InternalSpan internalSpan(String operationName, RequestSpan parent) {
      TrackingInternalSpan span = new TrackingInternalSpan();
      spans.add(span);
      return span;
    }

    @Override
    public Mono<Void> start() {
      return Mono.empty();
    }

    @Override
    public Mono<Void> stop(Duration timeout) {
      return Mono.empty();
    }

    @Override
    public RequestSpan requestSpan(String operationName, RequestSpan parent) {
      return new TrackingRequestSpan();
    }
  }

  private static class TrackingInternalSpan implements InternalSpan {

    volatile boolean finished = false;

    @Override
    public void finish() {
      finished = true;
    }

    @Override
    public void requestContext(RequestContext ctx) {

    }

    @Override
    public RequestContext requestContext() {
      return null;
    }

    @Override
    public void startPayloadEncoding() {

    }

    @Override
    public void stopPayloadEncoding() {

    }

    @Override
    public void startDispatch() {

    }

    @Override
    public void stopDispatch() {

    }

    @Override
    public RequestSpan toRequestSpan() {
      return new TrackingRequestSpan();
    }
  }

  private static class TrackingRequestSpan implements RequestSpan {
    @Override
    public void finish() {

    }
  }

}
