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

import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.RequestTracer;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.core.msg.RequestContext;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@IgnoreWhen(isProtostellarWillWorkLater = true)
class RequestTracerIntegrationTest extends JavaIntegrationTest {

  static private Cluster cluster;
  static private Collection collection;
  static private TrackingRequestTracer requestTracer;

  @BeforeAll
  static void beforeAll() {
    requestTracer = new TrackingRequestTracer();
    cluster = createCluster(env -> env.requestTracer(requestTracer));
    Bucket bucket = cluster.bucket(config().bucketname());
    collection = bucket.defaultCollection();

    bucket.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);
  }

  @AfterAll
  static void afterAll() {
    cluster.disconnect();
  }

  @Test
  void sendsStartAndEndRequestEventsOnSuccess() {
    String id = UUID.randomUUID().toString();
    collection.upsert(id, JsonObject.create());
    collection.get(id);

    int totalFinished = 0;
    for (TrackingRequestSpan span : requestTracer.spans) {
      assertTrue(span.finished);
      totalFinished++;
    }
    assertTrue(totalFinished > 0);
  }

  @Test
  void sendsStartAndEndRequestEventsOnFailure() {
    String id = UUID.randomUUID().toString();
    collection.upsert(id, JsonObject.create());
    try {
      collection.get(id, GetOptions.getOptions().timeout(Duration.ofNanos(1)));
      fail("Expected timeout exception");
    } catch (TimeoutException ex) {
      assertTrue(true);
    }

    int totalFinished = 0;
    for (TrackingRequestSpan span : requestTracer.spans) {
      assertTrue(span.finished);
      totalFinished++;
    }
    assertTrue(totalFinished > 0);
  }

  private static class TrackingRequestTracer implements RequestTracer {
    final List<TrackingRequestSpan> spans = Collections.synchronizedList(new ArrayList<>());

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
      TrackingRequestSpan span = new TrackingRequestSpan();
      spans.add(span);
      return span;
    }
  }

  private static class TrackingRequestSpan implements RequestSpan {

    volatile boolean finished = false;

    @Override
    public void attribute(String key, String value) {
    }

    @Override
    public void attribute(String key, boolean value) {
    }

    @Override
    public void attribute(String key, long value) {
    }

    @Override
    public void event(String name, Instant timestamp) {
    }

    @Override
    public void status(StatusCode status) {
    }

    @Override
    public void requestContext(RequestContext requestContext) {
    }

    @Override
    public void end() {
      finished = true;
    }
  }

}
