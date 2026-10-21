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

package com.couchbase.client.core.msg.query;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.cnc.tracing.NoopRequestTracer;
import com.couchbase.client.core.msg.BlockingCaller;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.core.util.MockUtil.mockCore;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QueryRequestTest {

  private final Core core = mockCore();

  private QueryRequest newQueryRequest() {
    return new QueryRequest(
      Duration.ofSeconds(10),
      core.context(),
      BestEffortRetryStrategy.INSTANCE,
      core.context().authenticator(),
      "SELECT 1",
      "{\"statement\":\"SELECT 1\"}".getBytes(UTF_8),
      true,
      null,
      null,
      null,
      null,
      null,
      false
    );
  }

  @Test
  void copiesDoTheirWorkOnTheOriginalsBlockingCaller() {
    // A prepared query sends a copy of the original request, to prepare or execute the statement.
    QueryRequest original = newQueryRequest();
    BlockingCaller caller = new BlockingCaller();
    original.blockingCaller(caller);

    QueryRequest prepare = original.toPrepareRequest(true, NoopRequestTracer.INSTANCE);
    QueryRequest execute = original.toExecuteRequest("p1", null, NoopRequestTracer.INSTANCE);
    assertSame(caller, prepare.blockingCaller());
    assertSame(caller, execute.blockingCaller());

    // The latest copy is the caller's current request: an interrupt cancels it, not the original (never sent).
    Thread.currentThread().interrupt();
    assertThrows(RuntimeException.class, () -> caller.await(new CompletableFuture<>()));
    assertEquals(CancellationReason.STOPPED_LISTENING, execute.cancellationReason());
    assertFalse(original.completed());
    assertFalse(prepare.completed());
  }

  @Test
  void copiesOfARequestWithoutABlockingCallerDoNotHaveOne() {
    QueryRequest original = newQueryRequest();
    assertNull(original.toExecuteRequest("p1", null, NoopRequestTracer.INSTANCE).blockingCaller());
    assertNull(original.toPrepareRequest(false, NoopRequestTracer.INSTANCE).blockingCaller());
  }

  @Test
  void copyMadeAfterTheCallerStoppedWaitingIsCancelled() {
    // For example, a prepared query retried on another thread after the caller was interrupted.
    QueryRequest original = newQueryRequest();
    BlockingCaller caller = new BlockingCaller();
    original.blockingCaller(caller);
    caller.await(CompletableFuture.completedFuture("done"));

    QueryRequest execute = original.toExecuteRequest("p1", null, NoopRequestTracer.INSTANCE);
    assertTrue(execute.completed());
    assertEquals(CancellationReason.STOPPED_LISTENING, execute.cancellationReason());
  }
}
