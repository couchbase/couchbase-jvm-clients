/*
 * Copyright (c) 2018 Couchbase, Inc.
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

import com.couchbase.client.core.cnc.events.request.IndividualReplicaGetFailedEvent;
import com.couchbase.client.core.deps.com.google.common.collect.Sets;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.DocumentUnretrievableException;
import com.couchbase.client.core.error.UnambiguousTimeoutException;
import com.couchbase.client.java.kv.GetReplicaResult;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.util.JavaIntegrationTest;
import com.couchbase.client.test.ClusterType;
import com.couchbase.client.test.IgnoreWhen;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.couchbase.client.core.util.CbCollections.setCopyOf;
import static com.couchbase.client.core.util.CbCollections.setOf;
import static com.couchbase.client.core.util.CbCollections.transform;
import static com.couchbase.client.test.Util.waitUntilCondition;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * This integration test verifies all different ways a replica read can be used.
 * <p>
 * Note that since naturally this depends on different topologies to be present (or not), you'll
 * find many annotations on the tests and the suite needs to be executed against different topologies
 * in order to execute them all.
 */
@IgnoreWhen(isProtostellarWillWorkLater = true) // Needs JVMCBC-1263
class ReplicaReadIntegrationTest extends JavaIntegrationTest {

  private static Cluster cluster;
  private static Collection collection;

  @BeforeAll
  static void setup() {
    cluster = createCluster();
    Bucket bucket = cluster.bucket(config().bucketname());
    collection = bucket.defaultCollection();
    bucket.waitUntilReady(WAIT_UNTIL_READY_DEFAULT);
  }

  @AfterAll
  static void tearDown() {
    cluster.disconnect();
  }

  /**
   * As a simple litmus test, when ALL is used there should be at least one record coming back,
   * namely from the active.
   * <p>
   * Depending on the topology, it might be more than that.
   */
  @Test
  void alwaysPassesWithAll() {
    String id = UUID.randomUUID().toString();

    collection.upsert(id, "Hello, World!");

    List<GetResult> results = collection.getAllReplicas(id).collect(Collectors.toList());
    assertFalse(results.isEmpty());
    for (GetResult result : results) {
      assertEquals("Hello, World!", result.contentAs(String.class));
      assertFalse(result.expiryTime().isPresent());
    }
  }

  /**
   * This test only executes when there are at least two nodes and exactly one replica
   * defined.
   * <p>
   * Note that this is supposed to work on the mock, but for some reason it does fail depending
   * on the key chosen. once https://github.com/couchbase/CouchbaseMock/issues/47 is cleared up
   * the restriction can be lifted.
   */
  @Test
  @IgnoreWhen(
    nodesLessThan = 2,
    replicasLessThan = 1,
    replicasGreaterThan = 1,
    clusterTypes = { ClusterType.MOCKED }
  )
  void twoResultsWithOneReplica() throws Exception {
    String id = UUID.randomUUID().toString();
    collection.upsert(id, "Hello, World!");

    // TODO: perform a "replicate to 1" instead
    Thread.sleep(1000);

    List<GetResult> results = collection.getAllReplicas(id).collect(Collectors.toList());
    assertEquals(2, results.size());
    assertNotNull(results.get(0));
    assertNotNull(results.get(1));
    assertEquals("Hello, World!", results.get(0).contentAs(String.class));
    assertEquals("Hello, World!", results.get(1).contentAs(String.class));
  }

  private static String absentId() {
    return "this document does not exist";
  }

  @Test
  void blockingGetAnyThrowsWhenNotFound() {
    assertThrows(DocumentUnretrievableException.class, () -> collection.getAnyReplica(absentId()));
  }

  @Test
  void blockingGetAllReturnsEmptyStreamWhenNotFound() {
    assertEquals(
      emptyList(),
      collection.getAllReplicas(absentId()).collect(Collectors.toList())
    );
  }

  @Test
  void asyncGetAnyReturnsFailedFutureWhenNotFound() throws InterruptedException {
    ExecutionException err = assertThrows(ExecutionException.class, () -> collection.async().getAnyReplica(absentId()).get());
    assertInstanceOf(DocumentUnretrievableException.class, err.getCause());

    // Also check the exception type seen in a "whenComplete" callback.
    // It's a different type: CompletionException vs ExecutionException.
    // Checking this because it's possible to get it wrong when using
    // the reactive API to emulate the current behavior of the async API,
    // which is something we may want to do for Protostellar.
    AtomicReference<Throwable> ref = new AtomicReference<>();
    CountDownLatch latch = new CountDownLatch(1);
    collection.async().getAnyReplica(absentId())
      .whenComplete((result, error) -> {
        ref.set(error != null ? error : new AssertionError("expected future to complete with error, but it did not."));
        latch.countDown();
      });
    assertTrue(latch.await(60, TimeUnit.SECONDS), "timed out waiting for count down latch");
    assertInstanceOf(CompletionException.class, ref.get());
    assertInstanceOf(DocumentUnretrievableException.class, ref.get().getCause());
  }

  @Test
  void asyncGetAllReturnsListOfFailedFuturesWhenNotFound() throws Exception {
    List<CompletableFuture<GetReplicaResult>> futures = collection.async().getAllReplicas(absentId()).get();

    // one result for each replica, plus 1 for active
    assertEquals(config().numReplicas() + 1, futures.size());

    List<Class<?>> errorClasses = transform(futures, future -> {
      ExecutionException e = assertThrows(ExecutionException.class, future::get);
      return e.getCause().getClass();
    });

    assertTrue(errorClasses.contains(DocumentNotFoundException.class), "expected at least one DocumentNotFoundException");

    Set<Class<?>> unexpectedExceptions = Sets.difference(
      setCopyOf(errorClasses),
      setOf(
        DocumentNotFoundException.class, // from online nodes
        UnambiguousTimeoutException.class // from offline nodes (as with CouchbaseMock)
      )
    );

    assertEquals(emptySet(), unexpectedExceptions, "unexpected exception");
  }

  @Test
  void reactiveGetAnyReturnsEmptyMonoWhenNotFound() throws Exception {
    assertNull(collection.reactive().getAnyReplica(absentId()).block());
  }

  @Test
  void reactiveGetAllReturnsEmptyFluxWhenNotFound() throws Exception {
    assertEquals(emptyList(),
        collection.reactive()
            .getAllReplicas(absentId())
            .collectList()
            .block());
  }

  @Test
  void reactiveGetAllFluxIsCold() throws Exception {
    String id = UUID.randomUUID().toString();

    Flux<GetReplicaResult> flux = collection.reactive().getAllReplicas(id);
    assertEquals(emptyList(), flux.collectList().block());

    collection.upsert(id, "Hello, World!");
    assertNotEquals(emptyList(), flux.collectList().block());
  }

  @Test
  void reactiveGetAnyMonoIsCold() throws Exception {
    String id = UUID.randomUUID().toString();

    Mono<GetReplicaResult> mono = collection.reactive().getAnyReplica(id);
    assertNull(mono.block());

    collection.upsert(id, "Hello, World!");
    assertNotNull(mono.block());
  }

  @Test
  void reactiveGetAllReturnsResult() throws Exception {
    String id = UUID.randomUUID().toString();
    collection.upsert(id, "Hello, World!");

    List<GetReplicaResult> results = collection.reactive()
        .getAllReplicas(id)
        .collectList()
        .block();

    assertNotNull(results);
    assertNotEquals(0, results.size());

    int primaryCount = 0;
    for (GetReplicaResult result : results) {
      if (!result.isReplica()) {
        primaryCount++;
      }
      assertEquals("Hello, World!", result.contentAs(String.class));
    }
    assertEquals(1, primaryCount);
  }

  @Test
  void reactiveGetAnyReturnsResult() throws Exception {
    String id = UUID.randomUUID().toString();
    collection.upsert(id, "Hello, World!");

    GetReplicaResult result = collection.reactive()
        .getAnyReplica(id)
        .block();

    assertNotNull(result);
    assertEquals("Hello, World!", result.contentAs(String.class));
  }

  /**
   * If we have a constellation where there are more replicas defined than available, a
   * subset of the requests will fail.
   * <p>
   * In ALL mode, these individual errors need to be ignored, but they should be logged at
   * warn level.
   * <p>
   * In this case we have 2 nodes and 2 replicas configured, but we'll only get the result
   * back from the active and one replica.
   */
  @Test
  @IgnoreWhen(
    nodesLessThan = 2,
    nodesGreaterThan = 2,
    replicasLessThan = 2,
    replicasGreaterThan = 2,
    clusterTypes = { ClusterType.MOCKED }
  )
  void ignoresErrorsOnNonAvailableReplicasInAllMode() throws Exception {
    final AtomicReference<IndividualReplicaGetFailedEvent> ev = new AtomicReference<>();
    cluster.environment().eventBus().subscribe(event -> {
      if (event instanceof IndividualReplicaGetFailedEvent) {
        ev.set((IndividualReplicaGetFailedEvent) event);
      }
    });

    String id = UUID.randomUUID().toString();
    collection.upsert(id, "Hello, World!");

    // TODO: perform a "replicate to 1" instead
    Thread.sleep(1000);

    List<GetResult> results = collection.getAllReplicas(id).collect(Collectors.toList());
    assertEquals(2, results.size());
    assertNotNull(results.get(0));
    assertNotNull(results.get(1));
    assertEquals("Hello, World!", results.get(0).contentAs(String.class));
    assertEquals("Hello, World!", results.get(1).contentAs(String.class));

    waitUntilCondition(() -> ev.get() != null);
  }

  // Checking behaviour used in getAnyReplica to make sure an aggregated future times out
  @Test
  void checkFuturesTimeout() {
    List<CompletableFuture<Integer>> futures = new ArrayList<>();

    CompletableFuture<Integer> cf1 = new CompletableFuture<>();
    CompletableFuture<Integer> cf2 = new CompletableFuture<>();

    futures.add(cf1);
    futures.add(cf2);

    cf1.completeExceptionally(new RuntimeException("argh!"));
    cf2.completeExceptionally(new RuntimeException("argh!"));

    CompletableFuture<Integer> f = new CompletableFuture<>();
    Consumer<Integer> complete = f::complete;
    futures.forEach(s -> s.thenAccept(complete));

    collection.core().context().environment().scheduler().schedule(() -> f.completeExceptionally(new TimeoutException()),
            50,
            TimeUnit.MILLISECONDS);

    ExecutionException err = assertThrows(ExecutionException.class, f::get);
    assertInstanceOf(TimeoutException.class, err.getCause());
  }

  // Checking behaviour used in reactive getAnyReplica
  @Test
  void oneMonoReturns() {
    Mono<Object> m1 = Mono.error(new RuntimeException("m1 failed")).onErrorResume(err -> Mono.empty());
    Mono<Object> m2 = Mono.error(new RuntimeException("m2 failed")).onErrorResume(err -> Mono.empty());
    Mono<Object> m3 = Mono.just(3);

    Flux<Object> flux = Flux.merge(m1, m2, m3);

    assertEquals(3, flux.next().block());
  }

  @Test
  void noMonoReturns() {
    Mono<Object> m1 = Mono.error(new RuntimeException("m1 failed")).onErrorResume(err -> Mono.empty());
    Mono<Object> m2 = Mono.error(new RuntimeException("m2 failed")).onErrorResume(err -> Mono.empty());

    Flux<Object> flux = Flux.merge(m1, m2);

    assertNull(flux.next().block());
  }

  @Test
  void noMonoReturnsErrorIfEmpty() {
    Mono<Object> m1 = Mono.error(new RuntimeException("m1 failed")).onErrorResume(err -> Mono.empty());
    Mono<Object> m2 = Mono.error(new RuntimeException("m2 failed")).onErrorResume(err -> Mono.empty());
    Mono<Object> m3 = Mono.just(3);

    Flux<Object> flux = Flux.merge(m1, m2)
            .switchIfEmpty(Mono.error(new NoSuchElementException()));

    assertThrows(NoSuchElementException.class, () -> flux.next().block());

    Flux<Object> flux2 = Flux.merge(m1, m2, m3)
            .switchIfEmpty(Mono.error(new NoSuchElementException()));

    assertEquals(3, flux2.next().block());

    Flux<Object> flux3 = Flux.merge(m1)
            .switchIfEmpty(Mono.error(new NoSuchElementException()));

    assertThrows(NoSuchElementException.class, () -> flux3.next().block());
  }
}
