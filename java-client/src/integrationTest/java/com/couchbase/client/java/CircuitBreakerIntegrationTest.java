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

package com.couchbase.client.java;

import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.core.diagnostics.EndpointDiagnostics;
import com.couchbase.client.core.endpoint.CircuitBreaker;
import com.couchbase.client.core.endpoint.CircuitBreakerConfig;
import com.couchbase.client.core.env.IoConfig;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.RequestCanceledException;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.io.netty.kv.KeyValueChannelContext;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.msg.kv.GetRequest;
import com.couchbase.client.core.msg.kv.GetResponse;
import com.couchbase.client.core.retry.FailFastRetryStrategy;
import com.couchbase.client.core.retry.RetryReason;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.couchbase.client.core.endpoint.CircuitBreaker.State.CLOSED;
import static com.couchbase.client.core.endpoint.CircuitBreaker.State.OPEN;
import static com.couchbase.client.core.util.CbCollections.setOf;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the default behavior of the circuit breaker when enabled.
 */
@IgnoreWhen(isProtostellarWillWorkLater = true)
class CircuitBreakerIntegrationTest extends JavaIntegrationTest {

  private Cluster cluster;
  private Collection collection;

  @BeforeEach
  void beforeEach() {
    cluster = createCluster(env -> env.ioConfig(IoConfig.kvCircuitBreakerConfig(CircuitBreakerConfig.enabled(true))));
    Bucket bucket = cluster.bucket(config().bucketname());
    collection = bucket.defaultCollection();
    bucket.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);
  }

  @AfterEach
  void afterEach() {
    cluster.disconnect();
  }

  /**
   * We need to make sure that other exceptions (like key not found) do not trigger the fail fast mechanism
   * of the circuit breaker, only those that are actually defined should open the circuit.
   */
  @Test
  void shouldNotTriggerWhenNotTimeouts() {
    int threshold = collection.environment().ioConfig().kvCircuitBreakerConfig().volumeThreshold();
    for (int i = 0; i < threshold * 3; i++) {
      assertThrows(DocumentNotFoundException.class, () -> collection.get("this-doc-does-not-exist"));
      assertEquals(setOf(CLOSED), circuitBreakerStates(ServiceType.KV));
    }
  }

  @Test
  void shouldTriggerWithTimeouts() {
    int threshold = collection.environment().ioConfig().kvCircuitBreakerConfig().volumeThreshold();

    int timeouts = 0;
    int cancellations = 0;
    for (int i = 0; i < threshold * 3; i++) {
      FailingGetRequestOnEncode request = new FailingGetRequestOnEncode(
        "foo",
        Duration.ofMillis(1),
        collection.core().context(),
        new CollectionIdentifier(
          config().bucketname(),
          Optional.of(collection.scopeName()),
          Optional.of(collection.name())
        ),
        FailFastRetryStrategy.INSTANCE
      );
      collection.core().send(request);


      ExecutionException ex = assertThrows(ExecutionException.class, () -> request.response().get());
      if (ex.getCause() instanceof TimeoutException) {
        timeouts++;
      } else if (ex.getCause() instanceof RequestCanceledException) {
        cancellations++;
        CancellationReason reason = ((RequestCanceledException) ex.getCause()).context().requestContext().request().cancellationReason();
        assertEquals(reason.innerReason(), RetryReason.ENDPOINT_CIRCUIT_OPEN);

        assertTrue(circuitBreakerStates(ServiceType.KV).contains(OPEN));
      }
    }

    assertTrue(timeouts > 0);
    assertTrue(cancellations > 0);
  }

  private static class FailingGetRequestOnEncode extends GetRequest {
    FailingGetRequestOnEncode(String key, Duration timeout, CoreContext ctx,
                              CollectionIdentifier collectionIdentifier, RetryStrategy retryStrategy) {
      super(key, timeout, ctx, collectionIdentifier, retryStrategy, null);
    }

    @Override
    public GetResponse decode(ByteBuf response, KeyValueChannelContext ctx) {
      this.cancel(CancellationReason.TIMEOUT);
      return super.decode(response, ctx);
    }
  }

  private Set<CircuitBreaker.State> circuitBreakerStates(ServiceType serviceType) {
    return cluster.diagnostics()
      .endpoints()
      .getOrDefault(serviceType, emptyList())
      .stream()
      .map(EndpointDiagnostics::circuitBreakerState)
      .collect(toSet());
  }
}
