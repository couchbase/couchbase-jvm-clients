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
import com.couchbase.client.core.cnc.RequestTracer;
import com.couchbase.client.core.cnc.metrics.AbstractMeter;
import com.couchbase.client.core.cnc.metrics.NoopMeter;
import com.couchbase.client.core.cnc.tracing.NoopRequestTracer;
import com.couchbase.client.core.cnc.tracing.TracingDecorator;
import com.couchbase.client.core.msg.Request;
import com.couchbase.client.core.msg.query.PreparedStatement;
import com.couchbase.client.core.msg.query.QueryRequest;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;

import static com.couchbase.client.core.util.MockUtil.mockCore;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class PreparedStatementStrategyTest {
  private static final String STATEMENT = "SELECT 1";

  private Core core;
  private RequestTracer tracer;

  @BeforeEach
  void setup() {
    core = mockCore();

    tracer = mock(RequestTracer.class);
    when(tracer.requestSpan(anyString(), any())).thenAnswer(invocation ->
        NoopRequestTracer.INSTANCE.requestSpan(invocation.getArgument(0), invocation.getArgument(1)));

    CoreResources resources = new CoreResources(
        tracer,
        new TracingDecorator(new ArrayList<>()),
        new AbstractMeter(NoopMeter.INSTANCE, new ArrayList<>())
    );
    when(core.context().coreResources()).thenReturn(resources);
  }

  @Test
  void enhancedCacheHitDoesNotPrepare() {
    EnhancedPreparedStatementStrategy strategy = new EnhancedPreparedStatementStrategy(core, 10);
    strategy.cache.put(STATEMENT, PreparedStatement.enhanced("p1"));
    assertCacheHitDoesNotPrepare(strategy);
  }

  @Test
  void legacyCacheHitDoesNotPrepare() {
    LegacyPreparedStatementStrategy strategy = new LegacyPreparedStatementStrategy(core, 10);
    strategy.cache.put(STATEMENT, PreparedStatement.legacy("p1", "plan"));
    assertCacheHitDoesNotPrepare(strategy);
  }

  @Test
  void enhancedCacheMissPreparesOnlyWhenSubscribed() {
    assertCacheMissPreparesOnlyWhenSubscribed(new EnhancedPreparedStatementStrategy(core, 10));
  }

  @Test
  void legacyCacheMissPreparesOnlyWhenSubscribed() {
    assertCacheMissPreparesOnlyWhenSubscribed(new LegacyPreparedStatementStrategy(core, 10));
  }

  private void assertCacheHitDoesNotPrepare(PreparedStatementStrategy strategy) {
    strategy.execute(newQueryRequest()).subscribe();

    verify(tracer, never()).requestSpan(eq("prepare"), any());
    verify(tracer, times(1)).requestSpan(eq("execute"), any());
    assertEquals(STATEMENT, sentRequest().statement());
  }

  private void assertCacheMissPreparesOnlyWhenSubscribed(PreparedStatementStrategy strategy) {
    QueryRequest request = newQueryRequest();

    // Assembling the reactive chain must not have side effects.
    Mono<?> result = strategy.execute(request);
    verifyNoInteractions(tracer);
    verify(core, never()).send(any());

    result.subscribe();
    verify(tracer, times(1)).requestSpan(eq("prepare"), any());
    assertEquals("PREPARE " + STATEMENT, sentRequest().statement());
  }

  private QueryRequest sentRequest() {
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Request<?>> captor = ArgumentCaptor.forClass(Request.class);
    verify(core, times(1)).send(captor.capture());
    return (QueryRequest) captor.getValue();
  }

  private QueryRequest newQueryRequest() {
    byte[] query = ("{\"statement\":\"" + STATEMENT + "\"}").getBytes(StandardCharsets.UTF_8);
    return new QueryRequest(
        Duration.ofSeconds(10),
        core.context(),
        BestEffortRetryStrategy.INSTANCE,
        core.context().authenticator(),
        STATEMENT,
        query,
        true,
        null,
        null,
        null,
        null,
        null,
        false
    );
  }
}
