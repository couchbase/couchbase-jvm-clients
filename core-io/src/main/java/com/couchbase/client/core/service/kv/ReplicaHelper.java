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
import com.couchbase.client.core.CoreKeyspace;
import com.couchbase.client.core.Reactor;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.kv.CoreKvResponseMetadata;
import com.couchbase.client.core.api.kv.CoreSubdocGetCommand;
import com.couchbase.client.core.api.kv.CoreSubdocGetResult;
import com.couchbase.client.core.api.kv.CoreReadPreference;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.cnc.events.request.IndividualReplicaGetFailedEvent;
import com.couchbase.client.core.config.BucketCapabilities;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.error.CommonExceptions;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DocumentUnretrievableException;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.error.context.AggregateErrorContext;
import com.couchbase.client.core.error.context.ErrorContext;
import com.couchbase.client.core.error.context.ReducedKeyValueErrorContext;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.kv.GetRequest;
import com.couchbase.client.core.msg.kv.GetResponse;
import com.couchbase.client.core.msg.kv.ReplicaGetRequest;
import com.couchbase.client.core.msg.kv.ReplicaSubdocGetRequest;
import com.couchbase.client.core.msg.kv.SubdocGetRequest;
import com.couchbase.client.core.msg.kv.SubdocGetResponse;
import com.couchbase.client.core.retry.RetryStrategy;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
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
      RequestSpan parentSpan,
      CoreReadPreference readPreference
  ) {
    notNullOrEmpty(documentId, "Id", () -> ReducedKeyValueErrorContext.create(documentId, collectionIdentifier));

    RequestSpan getAllSpan = core.context().coreResources().requestTracer().requestSpan(TracingIdentifiers.SPAN_GET_ALL_REPLICAS, parentSpan);

    return Reactor
        .toMono(() -> getAllReplicasRequests(core, collectionIdentifier, documentId, clientContext, retryStrategy, timeout, getAllSpan, readPreference))
        .flux()
        .flatMap(Flux::fromStream)
        .flatMap(request -> Reactor
            .wrap(request, get(core, request), true)
            .onErrorResume(t -> {
              core.environment().eventBus().publish(new IndividualReplicaGetFailedEvent(request.context()));
              return Mono.empty(); // Swallow any errors from individual replicas
            })
            .map(response -> new GetReplicaResponse(response, request instanceof ReplicaGetRequest))
        )
        .doFinally(signalType -> getAllSpan.end());
  }

  /**
   * @param core the core to execute the request
   * @param collectionIdentifier the collection containing the document
   * @param documentId the ID of the document
   * @param commands specifies the type of lookups to perform
   * @param timeout the timeout until we need to stop the get all replicas
   * @param retryStrategy the retry strategy to use
   * @param clientContext (nullable) client context info
   * @param parentSpan the "lookupIn all/any replicas" request span
   * @return a flux of requests.
   */
  public static Flux<CoreSubdocGetResult> lookupInAllReplicasReactive(
    final Core core,
    final CollectionIdentifier collectionIdentifier,
    final String documentId,
    final List<CoreSubdocGetCommand> commands,
    final Duration timeout,
    final RetryStrategy retryStrategy,
    Map<String, Object> clientContext,
    RequestSpan parentSpan,
    CoreReadPreference readPreference
  ) {
    notNullOrEmpty(documentId, "Id", () -> ReducedKeyValueErrorContext.create(documentId, collectionIdentifier));

    CoreEnvironment env = core.context().environment();
    RequestSpan getAllSpan = core.context().coreResources().requestTracer().requestSpan(TracingIdentifiers.SPAN_LOOKUP_IN_ALL_REPLICAS, parentSpan);
    return Reactor
      .toMono(() -> lookupInAllReplicasRequests(core, collectionIdentifier, documentId, commands, clientContext, retryStrategy, timeout, getAllSpan, readPreference))
      .flux()
      .flatMap(Flux::fromStream)
      .flatMap(request -> Reactor
        .wrap(request, get(core, request), true)
        .onErrorResume(t -> {
          env.eventBus().publish(new IndividualReplicaGetFailedEvent(request.context()));
          return Mono.empty(); // Swallow any errors from individual replicas
        })
        .map(response -> new CoreSubdocGetResult(CoreKeyspace.from(collectionIdentifier), documentId, CoreKvResponseMetadata.from(response.flexibleExtras()), Arrays.asList(response.values()), response.cas(), response.isDeleted(), request instanceof ReplicaSubdocGetRequest))
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
      final CoreReadPreference readPreference,
      final Function<GetReplicaResponse, R> responseMapper
  ) {
    RequestSpan getAllSpan = core.context().coreResources().requestTracer().requestSpan(TracingIdentifiers.SPAN_LOOKUP_IN_ALL_REPLICAS, parentSpan);

    return getAllReplicasRequests(core, collectionIdentifier, documentId, clientContext, retryStrategy, timeout, getAllSpan, readPreference)
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
   * Reads from replicas or the active node based on the options and returns the results as a list
   * of futures that might complete or fail.
   *
   * @param core the core to execute the request
   * @param collectionIdentifier the collection containing the document
   * @param documentId the ID of the document
   * @param commands specifies the type of lookups to perform
   * @param timeout the timeout until we need to stop the get all replicas
   * @param retryStrategy the retry strategy to use
   * @param clientContext (nullable) client context info
   * @param parentSpan the "lookupIn all/any replicas" request span
   * @param responseMapper converts the GetReplicaResponse to the client's native result type
   * @return a list of results from the active and the replica.
   */
  public static <R> CompletableFuture<List<CompletableFuture<R>>> lookupInAllReplicasAsync(
    final Core core,
    final CollectionIdentifier collectionIdentifier,
    final String documentId,
    final List<CoreSubdocGetCommand> commands,
    final Duration timeout,
    final RetryStrategy retryStrategy,
    final Map<String, Object> clientContext,
    final RequestSpan parentSpan,
    final CoreReadPreference readPreference,
    final Function<CoreSubdocGetResult, R> responseMapper
  ) {
    RequestSpan getAllSpan = core.context().coreResources().requestTracer().requestSpan(TracingIdentifiers.SPAN_GET_ALL_REPLICAS, parentSpan);

    return lookupInAllReplicasRequests(core, collectionIdentifier, documentId, commands, clientContext, retryStrategy, timeout, getAllSpan, readPreference)
      .thenApply(stream ->
        stream.map(request ->
          get(core, request)
            .thenApply(response -> new CoreSubdocGetResult(CoreKeyspace.from(collectionIdentifier), documentId, CoreKvResponseMetadata.from(response.flexibleExtras()), Arrays.asList(response.values()), response.cas(), response.isDeleted(), request instanceof ReplicaSubdocGetRequest))
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
      final CoreReadPreference readPreference,
      final Function<GetReplicaResponse, R> responseMapper) {

    RequestSpan getAnySpan = core.context().coreResources().requestTracer()
        .requestSpan(TracingIdentifiers.SPAN_GET_ANY_REPLICA, parentSpan);

    CompletableFuture<List<CompletableFuture<R>>> listOfFutures = getAllReplicasAsync(
        core, collectionIdentifier, documentId, timeout, retryStrategy, clientContext, getAnySpan, readPreference, responseMapper
    );

    // Aggregating the futures here will discard the individual errors, which we don't need
    CompletableFuture<R> anyReplicaFuture = aggregate(listOfFutures, responseMapper);
    return anyReplicaFuture.whenComplete((getReplicaResult, throwable) -> getAnySpan.end());
  }

  /**
   * @param core the core to execute the request
   * @param collectionIdentifier the collection containing the document
   * @param documentId the ID of the document
   * @param commands specifies the type of lookups to perform
   * @param timeout the timeout until we need to stop the get all replicas
   * @param retryStrategy the retry strategy to use
   * @param clientContext (nullable) client context info
   * @param parentSpan the "lookupIn all/any replicas" request span
   * @param responseMapper converts the CoreSubdocGetResult to the client's native result type
   * @return a list of results from the active and the replica.
   */
  public static <R> CompletableFuture<R> lookupInAnyReplicaAsync(
    final Core core,
    final CollectionIdentifier collectionIdentifier,
    final String documentId,
    final List<CoreSubdocGetCommand> commands,
    final Duration timeout,
    final RetryStrategy retryStrategy,
    final Map<String, Object> clientContext,
    final RequestSpan parentSpan,
    final CoreReadPreference readPreference,
    final Function<CoreSubdocGetResult, R> responseMapper) {

    RequestSpan getAnySpan = core.context().coreResources().requestTracer()
      .requestSpan(TracingIdentifiers.SPAN_LOOKUP_IN_ANY_REPLICA, parentSpan);

    CompletableFuture<List<CompletableFuture<R>>> listOfFutures = lookupInAllReplicasAsync(
      core, collectionIdentifier, documentId, commands, timeout, retryStrategy, clientContext, getAnySpan, readPreference, responseMapper
    );

    // Aggregating the futures here will discard the individual errors, which we don't need
    CompletableFuture<R> anyReplicaFuture = aggregate(listOfFutures, responseMapper);

    return anyReplicaFuture.whenComplete((lookupInReplicaResult, throwable) -> getAnySpan.end());
  }

  static private <R> CompletableFuture<R> aggregate(final CompletableFuture<List<CompletableFuture<R>>> listOfFutures,
                                                    final Function<?, R> responseMapper) { // only needed for type parameter
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
        try { // anyReplicaFuture must complete even if there is an unexpected error. Otherwise the caller hangs.
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
        } catch (Throwable t) {
            anyReplicaFuture.completeExceptionally(new RuntimeException(t.getClass().toString()));
        }
      }));
    });
    return anyReplicaFuture;
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
      final RequestSpan parent,
      final CoreReadPreference readPreference
  ) {
    notNullOrEmpty(documentId, "Id");

    final CoreContext coreContext = core.context();
    final CoreEnvironment environment = coreContext.environment();
    final BucketConfig config = core.clusterConfig().bucketConfig(collectionIdentifier.bucket());

    if (config instanceof CouchbaseBucketConfig) {
      CouchbaseBucketConfig topology = (CouchbaseBucketConfig) config;
      int numReplicas = topology.numberOfReplicas();
      List<GetRequest> requests = new ArrayList<>(numReplicas + 1);
      NodeIndexCalculator allowedNodeIndexes = new NodeIndexCalculator(readPreference, topology, coreContext);

      if (allowedNodeIndexes.canUseNodeForActive(documentId)) {
        RequestSpan span = coreContext.coreResources().requestTracer().requestSpan(TracingIdentifiers.SPAN_REQUEST_KV_GET, parent);
        GetRequest activeRequest = new GetRequest(documentId, timeout, coreContext, collectionIdentifier, retryStrategy, span);
        activeRequest.context().clientContext(clientContext);
        requests.add(activeRequest);
      }

      for (short replica = 1; replica <= numReplicas; replica++) {
        if (allowedNodeIndexes.canUseNodeForReplica(documentId, replica - 1)) {
          RequestSpan replicaSpan = coreContext.coreResources().requestTracer().requestSpan(TracingIdentifiers.SPAN_REQUEST_KV_GET_REPLICA, parent);
          ReplicaGetRequest replicaRequest = new ReplicaGetRequest(
            documentId, timeout, coreContext, collectionIdentifier, retryStrategy, replica, replicaSpan
          );
          replicaRequest.context().clientContext(clientContext);
          requests.add(replicaRequest);
        }
      }
      if (requests.isEmpty()) {
        CompletableFuture<Stream<GetRequest>> future = new CompletableFuture<>();
        future.completeExceptionally(DocumentUnretrievableException.noReplicasSuitable());
        return future;
      }
      return CompletableFuture.completedFuture(requests.stream());
    } else if (config == null) {
      // no bucket config found, it might be in-flight being opened so we need to reschedule the operation until
      // the timeout fires!
      final Duration retryDelay = Duration.ofMillis(100);
      final CompletableFuture<Stream<GetRequest>> future = new CompletableFuture<>();
      coreContext.environment().timer().schedule(() -> {
        getAllReplicasRequests(core, collectionIdentifier, documentId, clientContext, retryStrategy, timeout.minus(retryDelay), parent, readPreference).whenComplete((getRequestStream, throwable) -> {
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

  /**
   * Helper method to assemble a stream of requests to the active and all replicas
   *
   * @param core the core to execute the request
   * @param collectionIdentifier the collection containing the document
   * @param documentId the ID of the document
   * @param commands specifies the type of lookups to perform
   * @param clientContext (nullable) client context info
   * @param retryStrategy the retry strategy to use
   * @param timeout the timeout until we need to stop the get all replicas
   * @param parent the "get all/any replicas" request span
   * @return a stream of requests.
   */
  public static CompletableFuture<Stream<SubdocGetRequest>> lookupInAllReplicasRequests(
    final Core core,
    final CollectionIdentifier collectionIdentifier,
    final String documentId,
    final List<CoreSubdocGetCommand> commands,
    final Map<String, Object> clientContext,
    final RetryStrategy retryStrategy,
    final Duration timeout,
    final RequestSpan parent,
    final CoreReadPreference readPreference
  ) {
    notNullOrEmpty(documentId, "Id");

    final CoreContext coreContext = core.context();
    final CoreEnvironment environment = coreContext.environment();
    final BucketConfig config = core.clusterConfig().bucketConfig(collectionIdentifier.bucket());

    if (config instanceof CouchbaseBucketConfig) {
      CouchbaseBucketConfig topology = (CouchbaseBucketConfig) config;

      if (!config.bucketCapabilities().contains(BucketCapabilities.SUBDOC_READ_REPLICA)) {
        return failedFuture(FeatureNotAvailableException.subdocReadReplica());
      }

      int numReplicas = topology.numberOfReplicas();
      List<SubdocGetRequest> requests = new ArrayList<>(numReplicas + 1);
      NodeIndexCalculator allowedNodeIndexes = new NodeIndexCalculator(readPreference, topology, coreContext);

      if (allowedNodeIndexes.canUseNodeForActive(documentId)) {
        RequestSpan span = coreContext.coreResources().requestTracer().requestSpan(TracingIdentifiers.SPAN_REQUEST_KV_LOOKUP_IN, parent);
        SubdocGetRequest activeRequest = SubdocGetRequest.create(timeout, coreContext, collectionIdentifier, retryStrategy, documentId, (byte) 0, commands, span);
        activeRequest.context().clientContext(clientContext);
        requests.add(activeRequest);
      }

      for (short replica = 1; replica <= numReplicas; replica++) {
        if (allowedNodeIndexes.canUseNodeForReplica(documentId, replica - 1)) {
          RequestSpan replicaSpan = coreContext.coreResources().requestTracer().requestSpan(TracingIdentifiers.SPAN_LOOKUP_IN_ALL_REPLICAS, parent);
          ReplicaSubdocGetRequest replicaRequest = ReplicaSubdocGetRequest.create(
            timeout, coreContext, collectionIdentifier, retryStrategy, documentId, (byte) 0, commands, replica, replicaSpan
          );
          replicaRequest.context().clientContext(clientContext);
          requests.add(replicaRequest);
        }
      }
      if (requests.isEmpty()) {
        CompletableFuture<Stream<SubdocGetRequest>> future = new CompletableFuture<>();
        future.completeExceptionally(DocumentUnretrievableException.noReplicasSuitable());
        return future;
      }
      return CompletableFuture.completedFuture(requests.stream());
    } else if (config == null) {
      // no bucket config found, it might be in-flight being opened so we need to reschedule the operation until
      // the timeout fires!
      final Duration retryDelay = Duration.ofMillis(100);
      final CompletableFuture<Stream<SubdocGetRequest>> future = new CompletableFuture<>();
      coreContext.environment().timer().schedule(() -> {
        lookupInAllReplicasRequests(core, collectionIdentifier, documentId, commands, clientContext, retryStrategy, timeout.minus(retryDelay), parent, readPreference).whenComplete((getRequestStream, throwable) -> {
          if (throwable != null) {
            future.completeExceptionally(throwable);
          } else {
            future.complete(getRequestStream);
          }
        });
      }, retryDelay);
      return future;
    } else {
      return failedFuture(CommonExceptions.getFromReplicaNotCouchbaseBucket());
    }
  }

  private static <T> CompletableFuture<T> failedFuture(Throwable t) {
    CompletableFuture<T> future = new CompletableFuture<>();
    future.completeExceptionally(t);
    return future;
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

  private static CompletableFuture<SubdocGetResponse> get(final Core core, final SubdocGetRequest request) {
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
