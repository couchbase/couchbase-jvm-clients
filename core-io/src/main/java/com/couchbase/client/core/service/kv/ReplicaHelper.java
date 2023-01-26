/*
 * Copyright 2021 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.core.service.kv;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.Reactor;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.cnc.events.request.IndividualReplicaGetFailedEvent;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.error.CommonExceptions;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DocumentUnretrievableException;
import com.couchbase.client.core.error.context.AggregateErrorContext;
import com.couchbase.client.core.error.context.ErrorContext;
import com.couchbase.client.core.error.context.ReducedKeyValueErrorContext;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.kv.GetRequest;
import com.couchbase.client.core.msg.kv.GetResponse;
import com.couchbase.client.core.msg.kv.ReplicaGetRequest;
import com.couchbase.client.core.retry.RetryStrategy;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.couchbase.client.core.error.DefaultErrorUtil.keyValueStatusToException;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;
import static java.util.Objects.requireNonNull;

@Stability.Internal
public class ReplicaHelper {
  private ReplicaHelper() {
    throw new AssertionError("not instantiable");
  }

  /**
   * @deprecated Please use {@link com.couchbase.client.core.api.kv.CoreGetResult} in new code.
   */
  @Deprecated
  public static class GetReplicaResponse {
    private final GetResponse response;
    private final boolean fromReplica;

    public GetReplicaResponse(GetResponse response, boolean fromReplica) {
      this.response = requireNonNull(response);
      this.fromReplica = fromReplica;
    }

    public boolean isFromReplica() {
      return fromReplica;
    }

    public GetResponse getResponse() {
      return response;
    }
  }

  /**
   * @param clientContext (nullable)
   * @param parentSpan (nullable)
   */
  public static Flux<GetReplicaResponse> getAllReplicasReactive(
      final Core core,
      final CollectionIdentifier collectionIdentifier,
      final String documentId,
      final Duration timeout,
      final RetryStrategy retryStrategy,
      Map<String, Object> clientContext,
      RequestSpan parentSpan
  ) {
    notNullOrEmpty(documentId, "Id", () -> ReducedKeyValueErrorContext.create(documentId, collectionIdentifier));

    CoreEnvironment env = core.context().environment();
    RequestSpan getAllSpan = env.requestTracer().requestSpan(TracingIdentifiers.SPAN_GET_ALL_REPLICAS, parentSpan);
    getAllSpan.attribute(TracingIdentifiers.ATTR_SYSTEM, TracingIdentifiers.ATTR_SYSTEM_COUCHBASE);

    return Reactor
        .toMono(() -> getAllReplicasRequests(core, collectionIdentifier, documentId, clientContext, retryStrategy, timeout, getAllSpan))
        .flux()
        .flatMap(Flux::fromStream)
        .flatMap(request -> Reactor
            .wrap(request, get(core, request), true)
            .onErrorResume(t -> {
              env.eventBus().publish(new IndividualReplicaGetFailedEvent(request.context()));
              return Mono.empty(); // Swallow any errors from individual replicas
            })
            .map(response -> new GetReplicaResponse(response, request instanceof ReplicaGetRequest))
        )
        .doFinally(signalType -> getAllSpan.end());
  }

  /**
   * Reads from replicas or the active node based on the options and returns the results as a list
   * of futures that might complete or fail.
   *
   * @param clientContext (nullable)
   * @param parentSpan (nullable)
   * @param responseMapper converts the GetReplicaResponse to the client's native result type
   * @return a list of results from the active and the replica.
   */
  public static <R> CompletableFuture<List<CompletableFuture<R>>> getAllReplicasAsync(
      final Core core,
      final CollectionIdentifier collectionIdentifier,
      final String documentId,
      final Duration timeout,
      final RetryStrategy retryStrategy,
      final Map<String, Object> clientContext,
      final RequestSpan parentSpan,
      final Function<GetReplicaResponse, R> responseMapper
  ) {
    CoreEnvironment env = core.context().environment();
    RequestSpan getAllSpan = env.requestTracer().requestSpan(TracingIdentifiers.SPAN_GET_ALL_REPLICAS, parentSpan);
    getAllSpan.attribute(TracingIdentifiers.ATTR_SYSTEM, TracingIdentifiers.ATTR_SYSTEM_COUCHBASE);

    return getAllReplicasRequests(core, collectionIdentifier, documentId, clientContext, retryStrategy, timeout, getAllSpan)
        .thenApply(stream ->
            stream.map(request ->
                get(core, request)
                    .thenApply(response -> new GetReplicaResponse(response, request instanceof ReplicaGetRequest))
                    .thenApply(responseMapper)
            ).collect(Collectors.toList()))
        .whenComplete((completableFutures, throwable) -> {
          final AtomicInteger toComplete = new AtomicInteger(completableFutures.size());
          for (CompletableFuture<R> cf : completableFutures) {
            cf.whenComplete((a, b) -> {
              if (toComplete.decrementAndGet() == 0) {
                getAllSpan.end();
              }
            });
          }
        });
  }

  /**
   * @param clientContext (nullable)
   * @param parentSpan (nullable)
   * @param responseMapper converts the GetReplicaResponse to the client's native result type
   */
  public static <R> CompletableFuture<R> getAnyReplicaAsync(
      final Core core,
      final CollectionIdentifier collectionIdentifier,
      final String documentId,
      final Duration timeout,
      final RetryStrategy retryStrategy,
      final Map<String, Object> clientContext,
      final RequestSpan parentSpan,
      final Function<GetReplicaResponse, R> responseMapper) {

    RequestSpan getAnySpan = core.context().environment().requestTracer()
        .requestSpan(TracingIdentifiers.SPAN_GET_ANY_REPLICA, parentSpan);

    CompletableFuture<List<CompletableFuture<R>>> listOfFutures = getAllReplicasAsync(
        core, collectionIdentifier, documentId, timeout, retryStrategy, clientContext, getAnySpan, responseMapper
    );

    // Aggregating the futures here will discard the individual errors, which we don't need
    CompletableFuture<R> anyReplicaFuture = new CompletableFuture<>();
    listOfFutures.whenComplete((futures, throwable) -> {
      if (throwable != null) {
        anyReplicaFuture.completeExceptionally(throwable);
      }

      final AtomicBoolean successCompleted = new AtomicBoolean(false);
      final AtomicInteger totalCompleted = new AtomicInteger(0);
      final List<ErrorContext> nestedContexts = Collections.synchronizedList(new ArrayList<>());
      futures.forEach(individual -> individual.whenComplete((result, error) -> {
        int completed = totalCompleted.incrementAndGet();
        if (error != null) {
          if (error instanceof CompletionException && error.getCause() instanceof CouchbaseException) {
            nestedContexts.add(((CouchbaseException) error.getCause()).context());
          }
        }
        if (result != null && successCompleted.compareAndSet(false, true)) {
          anyReplicaFuture.complete(result);
        }
        if (!successCompleted.get() && completed == futures.size()) {
          anyReplicaFuture.completeExceptionally(new DocumentUnretrievableException(new AggregateErrorContext(nestedContexts)));
        }
      }));
    });

    return anyReplicaFuture.whenComplete((getReplicaResult, throwable) -> getAnySpan.end());
  }


  /**
   * Helper method to assemble a stream of requests to the active and all replicas
   *
   * @param core the core to execute the request
   * @param collectionIdentifier the collection containing the document
   * @param documentId the ID of the document
   * @param clientContext (nullable) client context info
   * @param retryStrategy the retry strategy to use
   * @param timeout the timeout until we need to stop the get all replicas
   * @param parent the "get all/any replicas" request span
   * @return a stream of requests.
   */
  public static CompletableFuture<Stream<GetRequest>> getAllReplicasRequests(
      final Core core,
      final CollectionIdentifier collectionIdentifier,
      final String documentId,
      final Map<String, Object> clientContext,
      final RetryStrategy retryStrategy,
      final Duration timeout,
      final RequestSpan parent
  ) {
    notNullOrEmpty(documentId, "Id");

    final CoreContext coreContext = core.context();
    final CoreEnvironment environment = coreContext.environment();
    final BucketConfig config = core.clusterConfig().bucketConfig(collectionIdentifier.bucket());

    if (config instanceof CouchbaseBucketConfig) {
      int numReplicas = ((CouchbaseBucketConfig) config).numberOfReplicas();
      List<GetRequest> requests = new ArrayList<>(numReplicas + 1);

      RequestSpan span = environment.requestTracer().requestSpan(TracingIdentifiers.SPAN_REQUEST_KV_GET, parent);
      GetRequest activeRequest = new GetRequest(documentId, timeout, coreContext, collectionIdentifier, retryStrategy, span);
      activeRequest.context().clientContext(clientContext);
      requests.add(activeRequest);

      for (short replica = 1; replica <= numReplicas; replica++) {
        RequestSpan replicaSpan = environment.requestTracer().requestSpan(TracingIdentifiers.SPAN_REQUEST_KV_GET_REPLICA, parent);
        ReplicaGetRequest replicaRequest = new ReplicaGetRequest(
            documentId, timeout, coreContext, collectionIdentifier, retryStrategy, replica, replicaSpan
        );
        replicaRequest.context().clientContext(clientContext);
        requests.add(replicaRequest);
      }
      return CompletableFuture.completedFuture(requests.stream());
    } else if (config == null) {
      // no bucket config found, it might be in-flight being opened so we need to reschedule the operation until
      // the timeout fires!
      final Duration retryDelay = Duration.ofMillis(100);
      final CompletableFuture<Stream<GetRequest>> future = new CompletableFuture<>();
      coreContext.environment().timer().schedule(() -> {
        getAllReplicasRequests(core, collectionIdentifier, documentId, clientContext, retryStrategy, timeout.minus(retryDelay), parent).whenComplete((getRequestStream, throwable) -> {
          if (throwable != null) {
            future.completeExceptionally(throwable);
          } else {
            future.complete(getRequestStream);
          }
        });
      }, retryDelay);
      return future;
    } else {
      final CompletableFuture<Stream<GetRequest>> future = new CompletableFuture<>();
      future.completeExceptionally(CommonExceptions.getFromReplicaNotCouchbaseBucket());
      return future;
    }
  }

  private static CompletableFuture<GetResponse> get(final Core core, final GetRequest request) {
    core.send(request);
    return request
        .response()
        .thenApply(response -> {
          if (!response.status().success()) {
            throw keyValueStatusToException(request, response);
          }
          return response;
        })
        .whenComplete((t, e) -> request.context().logicallyComplete(e));
  }
}
