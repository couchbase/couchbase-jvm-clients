/*
 * Copyright 2026 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.classic.query;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreResources;
import com.couchbase.client.core.api.query.CoreQueryOptions;
import com.couchbase.client.core.cnc.EventBus;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.RequestTracer;
import com.couchbase.client.core.cnc.metrics.AbstractMeter;
import com.couchbase.client.core.cnc.metrics.NoopMeter;
import com.couchbase.client.core.cnc.tracing.TracingDecorator;
import com.couchbase.client.core.config.ConfigurationProvider;
import com.couchbase.client.core.endpoint.http.CoreCommonOptions;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.PreparedStatementFailureException;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.query.QueryChunkHeader;
import com.couchbase.client.core.msg.query.QueryChunkRow;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.msg.query.QueryResponse;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static com.couchbase.client.core.util.MockUtil.mockCore;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies the top-level "query" span is ended for non-adhoc queries,
 * whose original request is never dispatched itself.
 */
class ClassicCoreQueryOpsSpanTest {
  private Core core;
  private final List<NamedSpan> spans = new ArrayList<>();

  /**
   * Handles each dispatched (prepare or execute) request.
   */
  private Consumer<QueryRequest> server;

  private static class NamedSpan {
    final String name;
    final RequestSpan span = mock(RequestSpan.class);

    NamedSpan(String name) {
      this.name = name;
    }
  }

  @BeforeEach
  void setup() {
    CoreEnvironment env = mock(CoreEnvironment.class);
    when(env.eventBus()).thenReturn(mock(EventBus.class));
    when(env.scheduler()).thenReturn(Schedulers.parallel());
    when(env.timeoutConfig()).thenReturn(TimeoutConfig.create());

    core = mockCore(env);

    RequestTracer tracer = mock(RequestTracer.class);
    when(tracer.requestSpan(anyString(), any())).thenAnswer(invocation -> {
      NamedSpan span = new NamedSpan(invocation.getArgument(0));
      spans.add(span);
      return span.span;
    });
    CoreResources resources = new CoreResources(
        tracer,
        new TracingDecorator(new ArrayList<>()),
        new AbstractMeter(NoopMeter.INSTANCE, new ArrayList<>())
    );
    when(core.context().coreResources()).thenReturn(resources);

    ConfigurationProvider configurationProvider = mock(ConfigurationProvider.class);
    when(configurationProvider.configs()).thenReturn(Flux.empty());
    when(core.configurationProvider()).thenReturn(configurationProvider);

    doAnswer(invocation -> {
      server.accept(invocation.getArgument(0));
      return null;
    }).when(core).send(any());
  }

  @Test
  void endsQuerySpanOnSuccess() {
    server = request -> succeed(request);

    runNonAdhocQuery();

    RequestSpan querySpan = span("query");
    verify(querySpan, times(1)).end();
    verify(querySpan, never()).status(RequestSpan.StatusCode.ERROR);
  }

  @Test
  void endsQuerySpanOnFailure() {
    CouchbaseException failure = new CouchbaseException("simulated failure");
    server = request -> {
      if (isPrepare(request)) {
        succeed(request);
      } else {
        request.fail(failure);
      }
    };

    assertThrows(CouchbaseException.class, this::runNonAdhocQuery);

    RequestSpan querySpan = span("query");
    verify(querySpan, times(1)).end();
    verify(querySpan).recordException(failure);
    verify(querySpan).status(RequestSpan.StatusCode.ERROR);
  }

  @Test
  void endsQuerySpanOnceAfterPreparedStatementRetry() {
    AtomicInteger executeAttempts = new AtomicInteger();
    server = request -> {
      if (!isPrepare(request) && executeAttempts.incrementAndGet() == 1) {
        request.fail(new PreparedStatementFailureException(null, true));
      } else {
        succeed(request);
      }
    };

    runNonAdhocQuery();

    assertEquals(2, executeAttempts.get());
    RequestSpan querySpan = span("query");
    verify(querySpan, times(1)).end();
    verify(querySpan, never()).status(RequestSpan.StatusCode.ERROR);
  }

  private void runNonAdhocQuery() {
    CoreQueryOptions options = mock(CoreQueryOptions.class);
    when(options.adhoc()).thenReturn(false);
    when(options.commonOptions()).thenReturn(
        CoreCommonOptions.of(Duration.ofSeconds(10), BestEffortRetryStrategy.INSTANCE, null));

    new ClassicCoreQueryOps(core)
        .queryReactive("SELECT 1", options, null, null, null)
        .block(Duration.ofSeconds(10));
  }

  private RequestSpan span(String name) {
    List<NamedSpan> matches = spans.stream()
        .filter(it -> it.name.equals(name))
        .collect(toList());
    assertEquals(1, matches.size(), "Expected exactly one '" + name + "' span");
    return matches.get(0).span;
  }

  private static boolean isPrepare(QueryRequest request) {
    return request.statement().startsWith("PREPARE ");
  }

  /**
   * Completes the request the way the legacy prepared statement strategy expects:
   * a prepare returns the plan as a row, and an execute returns no rows.
   */
  private static void succeed(QueryRequest request) {
    Flux<QueryChunkRow> rows = isPrepare(request)
        ? Flux.just(new QueryChunkRow("{\"name\":\"p1\",\"encoded_plan\":\"plan\"}".getBytes(StandardCharsets.UTF_8)))
        : Flux.empty();
    QueryChunkHeader header = new QueryChunkHeader("request-id", Optional.empty(), Optional.empty(), Optional.empty());
    request.succeed(new QueryResponse(ResponseStatus.SUCCESS, header, rows, Mono.empty()));
  }
}
