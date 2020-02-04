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

import com.couchbase.client.core.Core;
import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.InternalSpan;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.core.error.context.AggregateErrorContext;
import com.couchbase.client.core.error.CommonExceptions;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DocumentUnretrievableException;
import com.couchbase.client.core.error.context.ErrorContext;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.error.context.ReducedKeyValueErrorContext;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.msg.kv.GetAndLockRequest;
import com.couchbase.client.core.msg.kv.GetAndTouchRequest;
import com.couchbase.client.core.msg.kv.GetMetaRequest;
import com.couchbase.client.core.msg.kv.GetRequest;
import com.couchbase.client.core.msg.kv.InsertRequest;
import com.couchbase.client.core.msg.kv.RemoveRequest;
import com.couchbase.client.core.msg.kv.ReplaceRequest;
import com.couchbase.client.core.msg.kv.ReplicaGetRequest;
import com.couchbase.client.core.msg.kv.SubdocCommandType;
import com.couchbase.client.core.msg.kv.SubdocGetRequest;
import com.couchbase.client.core.msg.kv.SubdocMutateRequest;
import com.couchbase.client.core.msg.kv.TouchRequest;
import com.couchbase.client.core.msg.kv.UnlockRequest;
import com.couchbase.client.core.msg.kv.UpsertRequest;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.codec.Transcoder;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.kv.CommonDurabilityOptions;
import com.couchbase.client.java.kv.ExistsAccessor;
import com.couchbase.client.java.kv.ExistsOptions;
import com.couchbase.client.java.kv.ExistsResult;
import com.couchbase.client.java.kv.GetAccessor;
import com.couchbase.client.java.kv.GetAllReplicasOptions;
import com.couchbase.client.java.kv.GetAndLockOptions;
import com.couchbase.client.java.kv.GetAndTouchOptions;
import com.couchbase.client.java.kv.GetAnyReplicaOptions;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.GetReplicaResult;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.InsertAccessor;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.LookupInAccessor;
import com.couchbase.client.java.kv.LookupInOptions;
import com.couchbase.client.java.kv.LookupInResult;
import com.couchbase.client.java.kv.LookupInSpec;
import com.couchbase.client.java.kv.MutateInAccessor;
import com.couchbase.client.java.kv.MutateInOptions;
import com.couchbase.client.java.kv.MutateInResult;
import com.couchbase.client.java.kv.MutateInSpec;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.PersistTo;
import com.couchbase.client.java.kv.RemoveAccessor;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.ReplaceAccessor;
import com.couchbase.client.java.kv.ReplaceOptions;
import com.couchbase.client.java.kv.StoreSemantics;
import com.couchbase.client.java.kv.TouchAccessor;
import com.couchbase.client.java.kv.TouchOptions;
import com.couchbase.client.java.kv.UnlockAccessor;
import com.couchbase.client.java.kv.UnlockOptions;
import com.couchbase.client.java.kv.UpsertAccessor;
import com.couchbase.client.java.kv.UpsertOptions;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;
import static com.couchbase.client.java.ReactiveCollection.DEFAULT_EXISTS_OPTIONS;
import static com.couchbase.client.java.ReactiveCollection.DEFAULT_GET_ALL_REPLICAS_OPTIONS;
import static com.couchbase.client.java.ReactiveCollection.DEFAULT_GET_AND_LOCK_OPTIONS;
import static com.couchbase.client.java.ReactiveCollection.DEFAULT_GET_AND_TOUCH_OPTIONS;
import static com.couchbase.client.java.ReactiveCollection.DEFAULT_GET_ANY_REPLICA_OPTIONS;
import static com.couchbase.client.java.ReactiveCollection.DEFAULT_GET_OPTIONS;
import static com.couchbase.client.java.ReactiveCollection.DEFAULT_INSERT_OPTIONS;
import static com.couchbase.client.java.ReactiveCollection.DEFAULT_LOOKUP_IN_OPTIONS;
import static com.couchbase.client.java.ReactiveCollection.DEFAULT_MUTATE_IN_OPTIONS;
import static com.couchbase.client.java.ReactiveCollection.DEFAULT_REMOVE_OPTIONS;
import static com.couchbase.client.java.ReactiveCollection.DEFAULT_REPLACE_OPTIONS;
import static com.couchbase.client.java.ReactiveCollection.DEFAULT_TOUCH_OPTIONS;
import static com.couchbase.client.java.ReactiveCollection.DEFAULT_UNLOCK_OPTIONS;
import static com.couchbase.client.java.ReactiveCollection.DEFAULT_UPSERT_OPTIONS;
import static com.couchbase.client.java.kv.GetAccessor.EXPIRATION_MACRO;

/**
 * The {@link AsyncCollection} provides basic asynchronous access to all collection APIs.
 *
 * <p>This type of API provides asynchronous support through the concurrency mechanisms
 * that ship with Java 8 and later, notably the async {@link CompletableFuture}. It is the
 * async mechanism with the lowest overhead (best performance) but also comes with less
 * bells and whistles as the {@link ReactiveCollection} for example.</p>
 *
 * <p>Most of the time we recommend using the {@link ReactiveCollection} unless you need the
 * last drop of performance or if you are implementing higher level primitives on top of this
 * one.</p>
 *
 * @since 3.0.0
 */
public class AsyncCollection {

  /**
   * Holds the underlying core which is used to dispatch operations.
   */
  private final Core core;

  /**
   * Holds the core context of the attached core.
   */
  private final CoreContext coreContext;

  /**
   * Holds the environment for this collection.
   */
  private final ClusterEnvironment environment;

  /**
   * The name of the collection.
   */
  private final String name;

  /**
   * The name of the bucket.
   */
  private final String bucket;

  /**
   * The name of the associated scope.
   */
  private final String scopeName;

  /**
   * Holds the async binary collection object.
   */
  private final AsyncBinaryCollection asyncBinaryCollection;

  /**
   * Stores information about the collection.
   */
  private final CollectionIdentifier collectionIdentifier;

  /**
   * Creates a new {@link AsyncCollection}.
   *
   * @param name the name of the collection.
   * @param scopeName the name of the scope associated.
   * @param core the core into which ops are dispatched.
   * @param environment the surrounding environment for config options.
   */
  AsyncCollection(final String name, final String scopeName, final String bucket,
                  final Core core, final ClusterEnvironment environment) {
    this.name = name;
    this.scopeName = scopeName;
    this.core = core;
    this.coreContext = core.context();
    this.environment = environment;
    this.bucket = bucket;
    this.collectionIdentifier = new CollectionIdentifier(bucket, Optional.of(scopeName), Optional.of(name));
    this.asyncBinaryCollection = new AsyncBinaryCollection(core, environment, collectionIdentifier);
  }

  /**
   * Provides access to the underlying {@link Core}.
   */
  @Stability.Volatile
  public Core core() {
    return core;
  }

  /**
   * Provides access to the underlying {@link ClusterEnvironment}.
   */
  public ClusterEnvironment environment() {
    return environment;
  }

  /**
   * The name of the collection in use.
   *
   * @return the name of the collection.
   */
  public String name() {
    return name;
  }

  /**
   * Returns the name of the bucket associated with this collection.
   */
  public String bucketName() {
    return bucket;
  }

  /**
   * Returns the name of the scope associated with this collection.
   */
  public String scopeName() {
    return scopeName;
  }

  /**
   * Provides access to the binary APIs, not used for JSON documents.
   *
   * @return the {@link AsyncBinaryCollection}.
   */
  public AsyncBinaryCollection binary() {
    return asyncBinaryCollection;
  }

  /**
   * Fetches a full document (or a projection of it) from a collection with default options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @return a {@link CompletableFuture} indicating once loaded or failed.
   */
  public CompletableFuture<GetResult> get(final String id) {
    return get(id, DEFAULT_GET_OPTIONS);
  }

  /**
   * Fetches a full document (or a projection of it) from a collection with custom options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param options custom options to change the default behavior.
   * @return a {@link CompletableFuture} completing once loaded or failed.
   */
  public CompletableFuture<GetResult> get(final String id, final GetOptions options) {
    notNull(options, "GetOptions", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    final GetOptions.Built opts = options.build();

    final Transcoder transcoder = opts.transcoder() == null ? environment.transcoder() : opts.transcoder();
    if (opts.projections().isEmpty() && !opts.withExpiry()) {
      return GetAccessor.get(core, fullGetRequest(id, opts), transcoder);
    } else {
      return GetAccessor.subdocGet(core, subdocGetRequest(id, opts), transcoder);
    }
  }

  /**
   * Helper method to create a get request for a full doc fetch.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param opts custom options to change the default behavior.
   * @return the get request.
   */
  @Stability.Internal
  GetRequest fullGetRequest(final String id, final GetOptions.Built opts) {
    notNullOrEmpty(id, "Id", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    Duration timeout = opts.timeout().orElse(environment.timeoutConfig().kvTimeout());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());

    InternalSpan span = environment.requestTracer().internalSpan(GetRequest.OPERATION_NAME, opts.parentSpan().orElse(null));
    GetRequest request = new GetRequest(id, timeout, coreContext, collectionIdentifier, retryStrategy, span);
    request.context().clientContext(opts.clientContext());
    return request;
  }

  /**
   * Helper method to create a get request for a subdoc fetch.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param opts custom options to change the default behavior.
   * @return the subdoc get request.
   */
  @Stability.Internal
  SubdocGetRequest subdocGetRequest(final String id, final GetOptions.Built opts) {
    try {
      notNullOrEmpty(id, "Id");

      if (opts.withExpiry()) {
        if (opts.projections().size() > 15) {
          throw InvalidArgumentException.fromMessage("Only a maximum of 16 fields can be "
            + "projected per request due to a server limitation (includes the expiration macro as one field).");
        }
      } else {
        if (opts.projections().size() > 16) {
          throw InvalidArgumentException.fromMessage("Only a maximum of 16 fields can be "
            + "projected per request due to a server limitation.");
        }
      }
    } catch (Exception cause) {
      throw new InvalidArgumentException(
        "Argument validation failed",
        cause,
        ReducedKeyValueErrorContext.create(id, collectionIdentifier)
      );
    }

    Duration timeout = opts.timeout().orElse(environment.timeoutConfig().kvTimeout());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());

    List<SubdocGetRequest.Command> commands = new ArrayList<>(16);

    if (!opts.projections().isEmpty()) {
      if (opts.projections().size() > 16) {
        throw new UnsupportedOperationException("Only a maximum of 16 fields can be "
          + "projected per request.");
      }

      List<String> projections = opts.projections();
      for (int i = 0; i < projections.size(); i ++) {
        commands.add(new SubdocGetRequest.Command(SubdocCommandType.GET, projections.get(i), false, commands.size()));
      }
    } else {
      commands.add(new SubdocGetRequest.Command(
        SubdocCommandType.GET_DOC,
        "",
        false,
        commands.size()
      ));
    }

    if (opts.withExpiry()) {
      // xattrs must go first
      commands.add(0, new SubdocGetRequest.Command(
              SubdocCommandType.GET,
              EXPIRATION_MACRO,
              true,
              commands.size()
      ));
    }

    InternalSpan span = environment.requestTracer().internalSpan(SubdocGetRequest.OPERATION_NAME, opts.parentSpan().orElse(null));
    SubdocGetRequest request = new SubdocGetRequest(
      timeout, coreContext, collectionIdentifier, retryStrategy, id, (byte) 0x00, commands, span
    );
    request.context().clientContext(opts.clientContext());
    return request;
  }

  /**
   * Fetches a full document and write-locks it for the given duration with default options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param lockTime how long to lock the document for.  Any values above 30 seconds will be
   *                 treated as 30 seconds.
   * @return a {@link CompletableFuture} completing once loaded or failed.
   */
  public CompletableFuture<GetResult> getAndLock(final String id, Duration lockTime) {
    return getAndLock(id, lockTime, DEFAULT_GET_AND_LOCK_OPTIONS);
  }

  /**
   * Fetches a full document and write-locks it for the given duration with custom options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param lockTime how long to lock the document for.  Any values above 30 seconds will be
   *                 treated as 30 seconds.
   * @param options custom options to change the default behavior.
   * @return a {@link CompletableFuture} completing once loaded or failed.
   */
  public CompletableFuture<GetResult> getAndLock(final String id, final Duration lockTime,
                                                 final GetAndLockOptions options) {
    notNull(options, "GetAndLockOptions", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    GetAndLockOptions.Built opts = options.build();
    final Transcoder transcoder = opts.transcoder() == null ? environment.transcoder() : opts.transcoder();
    return GetAccessor.getAndLock(core, getAndLockRequest(id, lockTime, opts), transcoder);
  }

  /**
   * Helper method to create the get and lock request.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param lockTime how long to lock the document for.  Any values above 30 seconds will be
   *                 treated as 30 seconds.
   * @param opts custom options to change the default behavior.
   * @return the get and lock request.
   */
  @Stability.Internal
  GetAndLockRequest getAndLockRequest(final String id, final Duration lockTime, final GetAndLockOptions.Built opts) {
    notNullOrEmpty(id, "Id", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    notNull(lockTime, "LockTime", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    Duration timeout = opts.timeout().orElse(environment.timeoutConfig().kvTimeout());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());

    InternalSpan span = environment.requestTracer().internalSpan(GetAndLockRequest.OPERATION_NAME, opts.parentSpan().orElse(null));
    GetAndLockRequest request = new GetAndLockRequest(
      id, timeout, coreContext, collectionIdentifier, retryStrategy, lockTime, span
    );
    request.context().clientContext(opts.clientContext());
    return request;
  }

  /**
   * Fetches a full document and resets its expiration time to the value provided with default
   * options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param expiry the new expiration time for the document.
   * @return a {@link CompletableFuture} completing once loaded or failed.
   */
  public CompletableFuture<GetResult> getAndTouch(final String id, final Duration expiry) {
    return getAndTouch(id, expiry, DEFAULT_GET_AND_TOUCH_OPTIONS);
  }

  /**
   * Fetches a full document and resets its expiration time to the value provided with custom
   * options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param expiry the new expiration time for the document.
   * @param options custom options to change the default behavior.
   * @return a {@link CompletableFuture} completing once loaded or failed.
   */
  public CompletableFuture<GetResult> getAndTouch(final String id, final Duration expiry,
                                                  final GetAndTouchOptions options) {
    notNull(options, "GetAndTouchOptions", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    GetAndTouchOptions.Built opts = options.build();
    final Transcoder transcoder = opts.transcoder() == null ? environment.transcoder() : opts.transcoder();
    return GetAccessor.getAndTouch(core, getAndTouchRequest(id, expiry, opts), transcoder);
  }

  /**
   * Helper method for get and touch requests.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param expiry the new expiration time for the document.
   * @param opts custom options to change the default behavior.
   * @return the get and touch request.
   */
  @Stability.Internal
  GetAndTouchRequest getAndTouchRequest(final String id, final Duration expiry, final GetAndTouchOptions.Built opts) {
    notNullOrEmpty(id, "Id", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    notNull(expiry, "Expiry", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));

    Duration timeout = opts.timeout().orElse(environment.timeoutConfig().kvTimeout());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());
    InternalSpan span = environment.requestTracer().internalSpan(GetAndTouchRequest.OPERATION_NAME, opts.parentSpan().orElse(null));
    GetAndTouchRequest request = new GetAndTouchRequest(
      id, timeout, coreContext, collectionIdentifier, retryStrategy, expiry, span
    );
    request.context().clientContext(opts.clientContext());
    return request;
  }

  /**
   * Reads from all available replicas and the active node and returns the results as a list
   * of futures that might complete or fail.
   *
   * @param id the document id.
   * @return a list of results from the active and the replica.
   */
  public CompletableFuture<List<CompletableFuture<GetReplicaResult>>> getAllReplicas(final String id) {
    return getAllReplicas(id, DEFAULT_GET_ALL_REPLICAS_OPTIONS);
  }

  /**
   * Reads from replicas or the active node based on the options and returns the results as a list
   * of futures that might complete or fail.
   *
   * @param id the document id.
   * @return a list of results from the active and the replica.
   */
  public CompletableFuture<List<CompletableFuture<GetReplicaResult>>> getAllReplicas(final String id,
                                                                                     final GetAllReplicasOptions options) {
    notNull(options, "GetAllReplicasOptions");
    GetAllReplicasOptions.Built opts = options.build();
    Transcoder transcoder = opts.transcoder() == null ? environment.transcoder() : opts.transcoder();

    Duration timeout = opts.timeout().orElse(environment.timeoutConfig().kvTimeout());
    RequestSpan parent = environment.requestTracer().requestSpan(
      "get_all_replicas",
      opts.parentSpan().orElse(null)
    );

    return getAllReplicasRequests(id, opts, timeout, parent)
      .thenApply(stream -> stream.map(request -> GetAccessor
        .get(core, request, transcoder)
        .thenApply(response -> GetReplicaResult.from(response, request instanceof ReplicaGetRequest)))
        .collect(Collectors.toList())
      )
      .whenComplete((completableFutures, throwable) -> parent.finish());
  }

  /**
   * Reads all available replicas, and returns the first found.
   *
   * @param id the document id.
   * @return a future containing the first available replica.
   */
  public CompletableFuture<GetReplicaResult> getAnyReplica(final String id) {
    return getAnyReplica(id, DEFAULT_GET_ANY_REPLICA_OPTIONS);
  }

  /**
   * Reads all available replicas, and returns the first found.
   *
   * @param id the document id.
   * @param options the custom options.
   * @return a future containing the first available replica.
   */
  public CompletableFuture<GetReplicaResult> getAnyReplica(final String id, final GetAnyReplicaOptions options) {
    notNullOrEmpty(id, "Id", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    notNull(options, "GetAnyReplicaOptions", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    GetAnyReplicaOptions.Built built = options.build();

    GetAllReplicasOptions opts = GetAllReplicasOptions.getAllReplicasOptions().clientContext(built.clientContext());
    built.timeout().ifPresent(opts::timeout);
    built.retryStrategy().ifPresent(opts::retryStrategy);
    if (built.transcoder() != null) {
      opts.transcoder(built.transcoder());
    }
    RequestSpan parent = environment.requestTracer().requestSpan(
      "get_any_replica",
      built.parentSpan().orElse(null)
    );
    opts.parentSpan(parent);


    CompletableFuture<List<CompletableFuture<GetReplicaResult>>> listOfFutures = getAllReplicas(id, opts);

    // Aggregating the futures here will discard the individual errors, which we don't need
    CompletableFuture<GetReplicaResult> anyReplicaFuture = new CompletableFuture<>();
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

    return anyReplicaFuture.whenComplete((getReplicaResult, throwable) -> parent.finish());
  }

  /**
   * Helper method to assemble a stream of requests either to the active or to the replica.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param opts custom options to change the default behavior.
   * @param timeout the timeout until we need to stop the get all replicas
   * @return a stream of requests.
   */
  CompletableFuture<Stream<GetRequest>> getAllReplicasRequests(final String id, final GetAllReplicasOptions.Built opts,
                                                               final Duration timeout, final RequestSpan parent) {
    notNullOrEmpty(id, "Id");
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());

    final BucketConfig config = core.clusterConfig().bucketConfig(bucket);

    if (config instanceof CouchbaseBucketConfig) {
      int numReplicas = ((CouchbaseBucketConfig) config).numberOfReplicas();
      List<GetRequest> requests = new ArrayList<>(numReplicas + 1);

      InternalSpan span = environment.requestTracer().internalSpan(GetRequest.OPERATION_NAME, parent);
      GetRequest activeRequest = new GetRequest(id, timeout, coreContext, collectionIdentifier, retryStrategy, span);
      activeRequest.context().clientContext(opts.clientContext());
      requests.add(activeRequest);

      for (int i = 0; i < numReplicas; i++) {
        InternalSpan replicaSpan = environment.requestTracer().internalSpan(ReplicaGetRequest.OPERATION_NAME, parent);
        ReplicaGetRequest replicaRequest = new ReplicaGetRequest(
          id, timeout, coreContext, collectionIdentifier, retryStrategy, (short) (i + 1), replicaSpan
        );
        replicaRequest.context().clientContext(opts.clientContext());
        requests.add(replicaRequest);
      }
      return CompletableFuture.completedFuture(requests.stream());
    } else if (config == null) {
      // no bucket config found, it might be in-flight being opened so we need to reschedule the operation until
      // the timeout fires!
      final Duration retryDelay = Duration.ofMillis(100);
      final CompletableFuture<Stream<GetRequest>> future = new CompletableFuture<>();
      coreContext.environment().timer().schedule(() -> {
        getAllReplicasRequests(id, opts, timeout.minus(retryDelay), parent).whenComplete((getRequestStream, throwable) -> {
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
   * Checks if the given document ID exists on the active partition with default options.
   *
   * @param id the document ID
   * @return a {@link CompletableFuture} completing once loaded or failed.
   */
  public CompletableFuture<ExistsResult> exists(final String id) {
    return exists(id, DEFAULT_EXISTS_OPTIONS);
  }

  /**
   * Checks if the given document ID exists on the active partition with custom options.
   *
   * @param id the document ID
   * @param options to modify the default behavior
   * @return a {@link CompletableFuture} completing once loaded or failed.
   */
  public CompletableFuture<ExistsResult> exists(final String id, final ExistsOptions options) {
    return ExistsAccessor.exists(id, core, existsRequest(id, options));
  }

  /**
   * Helper method to create the exists request from its options.
   *
   * @param id the document ID
   * @param options custom options to change the default behavior
   * @return the observe request used for exists.
   */
  GetMetaRequest existsRequest(final String id, final ExistsOptions options) {
    notNullOrEmpty(id, "Id", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    notNull(options, "ExistsOptions", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    ExistsOptions.Built opts = options.build();

    Duration timeout = opts.timeout().orElse(environment.timeoutConfig().kvTimeout());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());
    InternalSpan span = environment.requestTracer().internalSpan(GetMetaRequest.OPERATION_NAME_EXISTS, opts.parentSpan().orElse(null));
    GetMetaRequest request = new GetMetaRequest(id, timeout, coreContext, collectionIdentifier, retryStrategy, span);
    request.context().clientContext(opts.clientContext());
    return request;
  }


  /**
   * Removes a Document from a collection with default options.
   *
   * @param id the id of the document to remove.
   * @return a {@link CompletableFuture} completing once removed or failed.
   */
  public CompletableFuture<MutationResult> remove(final String id) {
    return remove(id, DEFAULT_REMOVE_OPTIONS);
  }

  /**
   * Removes a Document from a collection with custom options.
   *
   * @param id the id of the document to remove.
   * @param options custom options to change the default behavior.
   * @return a {@link CompletableFuture} completing once removed or failed.
   */
  public CompletableFuture<MutationResult> remove(final String id, final RemoveOptions options) {
    notNull(options, "RemoveOptions", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    RemoveOptions.Built opts = options.build();
    return RemoveAccessor.remove(core, removeRequest(id, opts), id, opts.persistTo(), opts.replicateTo());
  }

  /**
   * Helper method to create the remove request.
   *
   * @param id the id of the document to remove.
   * @param opts custom options to change the default behavior.
   * @return the remove request.
   */
  RemoveRequest removeRequest(final String id, final RemoveOptions.Built opts) {
    notNullOrEmpty(id, "Id", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    Duration timeout = decideKvTimeout(opts, environment.timeoutConfig());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());
    final InternalSpan span = environment
      .requestTracer()
      .internalSpan(RemoveRequest.OPERATION_NAME, opts.parentSpan().orElse(null));
    RemoveRequest request = new RemoveRequest(id, opts.cas(), timeout,
      coreContext, collectionIdentifier, retryStrategy, opts.durabilityLevel(), span);
    request.context().clientContext(opts.clientContext());
    return request;
  }

  /**
   * Inserts a full document which does not exist yet with default options.
   *
   * @param id the document id to insert.
   * @param content the document content to insert.
   * @return a {@link CompletableFuture} completing once inserted or failed.
   */
  public CompletableFuture<MutationResult> insert(final String id, Object content) {
    return insert(id, content, DEFAULT_INSERT_OPTIONS);
  }

  /**
   * Inserts a full document which does not exist yet with custom options.
   *
   * @param id the document id to insert.
   * @param content the document content to insert.
   * @param options custom options to customize the insert behavior.
   * @return a {@link CompletableFuture} completing once inserted or failed.
   */
  public CompletableFuture<MutationResult> insert(final String id, Object content, final InsertOptions options) {
    notNull(options, "InsertOptions", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    InsertOptions.Built opts = options.build();
    return InsertAccessor.insert(core, insertRequest(id, content, opts), id, opts.persistTo(), opts.replicateTo());
  }

  /**
   * Helper method to generate the insert request.
   *
   * @param id the document id to insert.
   * @param content the document content to insert.
   * @param opts custom options to customize the insert behavior.
   * @return the insert request.
   */
  InsertRequest insertRequest(final String id, final Object content, final InsertOptions.Built opts) {
    notNullOrEmpty(id, "Id", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    notNull(content, "Content", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));

    Duration timeout = decideKvTimeout(opts, environment.timeoutConfig());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());
    Transcoder transcoder = opts.transcoder() == null ? environment.transcoder() : opts.transcoder();

    final InternalSpan span = environment
      .requestTracer()
      .internalSpan(InsertRequest.OPERATION_NAME, opts.parentSpan().orElse(null));

    long start = System.nanoTime();
    Transcoder.EncodedValue encoded;
    try {
      span.startPayloadEncoding();
      encoded = transcoder.encode(content);
    } finally {
      span.stopPayloadEncoding();
    }
    long end = System.nanoTime();

    InsertRequest request = new InsertRequest(id, encoded.encoded(), opts.expiry().getSeconds(), encoded.flags(),
      timeout, coreContext, collectionIdentifier, retryStrategy, opts.durabilityLevel(), span);
    request.context()
      .clientContext(opts.clientContext())
      .encodeLatency(end - start);
    return request;
  }

  /**
   * Upserts a full document which might or might not exist yet with default options.
   *
   * @param id the document id to upsert.
   * @param content the document content to upsert.
   * @return a {@link CompletableFuture} completing once upserted or failed.
   */
  public CompletableFuture<MutationResult> upsert(final String id, Object content) {
    return upsert(id, content, DEFAULT_UPSERT_OPTIONS);
  }

  /**
   * Upserts a full document which might or might not exist yet with custom options.
   *
   * @param id the document id to upsert.
   * @param content the document content to upsert.
   * @param options custom options to customize the upsert behavior.
   * @return a {@link CompletableFuture} completing once upserted or failed.
   */
  public CompletableFuture<MutationResult> upsert(final String id, Object content, final UpsertOptions options) {
    notNull(options, "UpsertOptions", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    UpsertOptions.Built opts = options.build();
    return UpsertAccessor.upsert(core, upsertRequest(id, content, opts), id, opts.persistTo(), opts.replicateTo());
  }

  /**
   * Helper method to generate the upsert request.
   *
   * @param id the document id to upsert.
   * @param content the document content to upsert.
   * @param opts custom options to customize the upsert behavior.
   * @return the upsert request.
   */
  UpsertRequest upsertRequest(final String id, final Object content, final UpsertOptions.Built opts) {
    notNullOrEmpty(id, "Id", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    notNull(content, "Content", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));

    Duration timeout = decideKvTimeout(opts, environment.timeoutConfig());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());
    Transcoder transcoder = opts.transcoder() == null ? environment.transcoder() : opts.transcoder();

    final InternalSpan span = environment
      .requestTracer()
      .internalSpan(UpsertRequest.OPERATION_NAME, opts.parentSpan().orElse(null));

    long start = System.nanoTime();
    Transcoder.EncodedValue encoded;
    try {
      span.startPayloadEncoding();
      encoded = transcoder.encode(content);
    } finally {
      span.stopPayloadEncoding();
    }
    long end = System.nanoTime();

    final UpsertRequest request = new UpsertRequest(id, encoded.encoded(), opts.expiry().getSeconds(), encoded.flags(),
      timeout, coreContext, collectionIdentifier, retryStrategy, opts.durabilityLevel(), span);
    request.context()
      .clientContext(opts.clientContext())
      .encodeLatency(end - start);
    return request;
  }

  /**
   * Replaces a full document which already exists with default options.
   *
   * @param id the document id to replace.
   * @param content the document content to replace.
   * @return a {@link CompletableFuture} completing once replaced or failed.
   */
  public CompletableFuture<MutationResult> replace(final String id, Object content) {
    return replace(id, content, DEFAULT_REPLACE_OPTIONS);
  }

  /**
   * Replaces a full document which already exists with custom options.
   *
   * @param id the document id to replace.
   * @param content the document content to replace.
   * @param options custom options to customize the replace behavior.
   * @return a {@link CompletableFuture} completing once replaced or failed.
   */
  public CompletableFuture<MutationResult> replace(final String id, Object content, final ReplaceOptions options) {
    notNull(options, "ReplaceOptions", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    ReplaceOptions.Built opts = options.build();
    return ReplaceAccessor.replace(core, replaceRequest(id, content, opts), id, opts.persistTo(), opts.replicateTo());
  }

  /**
   * Helper method to generate the replace request.
   *
   * @param id the document id to replace.
   * @param content the document content to replace.
   * @param opts custom options to customize the replace behavior.
   * @return the replace request.
   */
  ReplaceRequest replaceRequest(final String id, final Object content, final ReplaceOptions.Built opts) {
    notNullOrEmpty(id, "Id", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    notNull(content, "Content", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));

    Duration timeout = decideKvTimeout(opts, environment.timeoutConfig());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());
    Transcoder transcoder = opts.transcoder() == null ? environment.transcoder() : opts.transcoder();

    final InternalSpan span = environment
      .requestTracer()
      .internalSpan(ReplaceRequest.OPERATION_NAME, opts.parentSpan().orElse(null));

    long start = System.nanoTime();
    Transcoder.EncodedValue encoded;
    try {
      span.startPayloadEncoding();
      encoded = transcoder.encode(content);
    } finally {
      span.stopPayloadEncoding();
    }
    long end = System.nanoTime();

    ReplaceRequest request = new ReplaceRequest(id, encoded.encoded(), opts.expiry().getSeconds(), encoded.flags(),
      timeout, opts.cas(), coreContext, collectionIdentifier, retryStrategy, opts.durabilityLevel(), span);
    request.context()
      .clientContext(opts.clientContext())
      .encodeLatency(end - start);
    return request;
  }

  /**
   * Updates the expiry of the document with the given id with default options.
   *
   * @param id the id of the document to update.
   * @param expiry the new expiry for the document.
   * @return a {@link MutationResult} once the operation completes.
   */
  public CompletableFuture<MutationResult> touch(final String id, final Duration expiry) {
    return touch(id, expiry, DEFAULT_TOUCH_OPTIONS);
  }

  /**
   * Updates the expiry of the document with the given id with custom options.
   *
   * @param id the id of the document to update.
   * @param expiry the new expiry for the document.
   * @param options the custom options.
   * @return a {@link MutationResult} once the operation completes.
   */
  public CompletableFuture<MutationResult> touch(final String id, final Duration expiry, final TouchOptions options) {
    return TouchAccessor.touch(core, touchRequest(id, expiry, options), id);
  }

  /**
   * Helper method to create the touch request.
   *
   * @param id the id of the document to update.
   * @param expiry the new expiry for the document.
   * @param options the custom options.
   * @return the touch request.
   */
  TouchRequest touchRequest(final String id, final Duration expiry, final TouchOptions options) {
    notNullOrEmpty(id, "Id", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    notNull(expiry, "Expiry", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    notNull(options, "TouchOptions", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    TouchOptions.Built opts = options.build();

    Duration timeout = opts.timeout().orElse(environment.timeoutConfig().kvTimeout());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());
    InternalSpan span = environment.requestTracer().internalSpan(TouchRequest.OPERATION_NAME, opts.parentSpan().orElse(null));
    TouchRequest request = new TouchRequest(timeout, coreContext, collectionIdentifier, retryStrategy, id,
      expiry.getSeconds(), span);
    request.context().clientContext(opts.clientContext());
    return request;
  }

  /**
   * Unlocks a document if it has been locked previously, with default options.
   *
   * @param id the id of the document.
   * @param cas the CAS value which is needed to unlock it.
   * @return the future which completes once a response has been received.
   */
  public CompletableFuture<Void> unlock(final String id, final long cas) {
    return unlock(id, cas, DEFAULT_UNLOCK_OPTIONS);
  }

  /**
   * Unlocks a document if it has been locked previously, with custom options.
   *
   * @param id the id of the document.
   * @param cas the CAS value which is needed to unlock it.
   * @param options the options to customize.
   * @return the future which completes once a response has been received.
   */
  public CompletableFuture<Void> unlock(final String id, final long cas, final UnlockOptions options) {
    return UnlockAccessor.unlock(id, core, unlockRequest(id, cas, options));
  }

  /**
   * Helper method to create the unlock request.
   *
   * @param id the id of the document.
   * @param cas the CAS value which is needed to unlock it.
   * @param options the options to customize.
   * @return the unlock request.
   */
  UnlockRequest unlockRequest(final String id, final long cas, final UnlockOptions options) {
    notNullOrEmpty(id, "Id", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    notNull(options, "UnlockOptions", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    if (cas == 0) {
      throw new InvalidArgumentException("CAS cannot be 0", null, ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    }
    UnlockOptions.Built opts = options.build();

    Duration timeout = opts.timeout().orElse(environment.timeoutConfig().kvTimeout());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());
    InternalSpan span = environment.requestTracer().internalSpan(UnlockRequest.OPERATION_NAME, opts.parentSpan().orElse(null));
    UnlockRequest request = new UnlockRequest(timeout, coreContext, collectionIdentifier, retryStrategy, id, cas, span);
    request.context().clientContext(opts.clientContext());
    return request;
  }

  /**
   * Performs lookups to document fragments with default options.
   *
   * @param id the outer document ID.
   * @param specs the spec which specifies the type of lookups to perform.
   * @return the {@link LookupInResult} once the lookup has been performed or failed.
   */
  public CompletableFuture<LookupInResult> lookupIn(final String id, final List<LookupInSpec> specs) {
    return lookupIn(id, specs, DEFAULT_LOOKUP_IN_OPTIONS);
  }

  /**
   * Performs lookups to document fragments with custom options.
   *
   * @param id the outer document ID.
   * @param specs the spec which specifies the type of lookups to perform.
   * @param options custom options to modify the lookup options.
   * @return the {@link LookupInResult} once the lookup has been performed or failed.
   */
  public CompletableFuture<LookupInResult> lookupIn(final String id, final List<LookupInSpec> specs,
                                                    final LookupInOptions options) {
    notNull(options, "LookupInOptions", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    LookupInOptions.Built opts = options.build();
    final JsonSerializer serializer = opts.serializer() == null ? environment.jsonSerializer() : opts.serializer();
    return LookupInAccessor.lookupInAccessor(core, lookupInRequest(id, specs, opts), serializer);
  }

  /**
   * Helper method to create the underlying lookup subdoc request.
   *
   * @param id the outer document ID.
   * @param specs the spec which specifies the type of lookups to perform.
   * @param opts custom options to modify the lookup options.
   * @return the subdoc lookup request.
   */
  SubdocGetRequest lookupInRequest(final String id, final List<LookupInSpec> specs, final LookupInOptions.Built opts) {
    notNullOrEmpty(id, "Id", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    notNullOrEmpty(specs, "LookupInSpecs", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));

    ArrayList<SubdocGetRequest.Command> commands = new ArrayList<>(specs.size());

    for (int i = 0; i < specs.size(); i ++) {
      LookupInSpec spec = specs.get(i);
      commands.add(spec.export(i));
    }

    // xattrs come first
    commands.sort(Comparator.comparing(v -> !v.xattr()));

    Duration timeout = opts.timeout().orElse(environment.timeoutConfig().kvTimeout());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());

    byte flags = 0;
    if (opts.accessDeleted()) {
      flags |= SubdocMutateRequest.SUBDOC_DOC_FLAG_ACCESS_DELETED;
    }

    InternalSpan span = environment.requestTracer().internalSpan(SubdocGetRequest.OPERATION_NAME, opts.parentSpan().orElse(null));
    SubdocGetRequest request = new SubdocGetRequest(timeout, coreContext, collectionIdentifier, retryStrategy, id,
      flags, commands, span);
    request.context().clientContext(opts.clientContext());
    return request;
  }

  /**
   * Performs mutations to document fragments with default options.
   *
   * @param id the outer document ID.
   * @param specs the spec which specifies the type of mutations to perform.
   * @return the {@link MutateInResult} once the mutation has been performed or failed.
   */
  public CompletableFuture<MutateInResult> mutateIn(final String id,
                                                    final List<MutateInSpec> specs) {
    return mutateIn(id, specs, DEFAULT_MUTATE_IN_OPTIONS);
  }

  /**
   * Performs mutations to document fragments with custom options.
   *
   * @param id the outer document ID.
   * @param specs the spec which specifies the type of mutations to perform.
   * @param options custom options to modify the mutation options.
   * @return the {@link MutateInResult} once the mutation has been performed or failed.
   */
  public CompletableFuture<MutateInResult> mutateIn(final String id,
                                                    final List<MutateInSpec> specs,
                                                    final MutateInOptions options) {
    notNull(options, "MutateInOptions", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    MutateInOptions.Built opts = options.build();
    return MutateInAccessor.mutateIn(
      core,
      mutateInRequest(id, specs, opts),
      id,
      opts.persistTo(),
      opts.replicateTo(),
      opts.storeSemantics() == StoreSemantics.INSERT,
      environment.jsonSerializer()
    );
  }

  /**
   * Helper method to create the underlying subdoc mutate request.
   *
   * @param id the outer document ID.
   * @param specs the spec which specifies the type of mutations to perform.
   * @param opts custom options to modify the mutation options.
   * @return the subdoc mutate request.
   */
  SubdocMutateRequest mutateInRequest(final String id, final List<MutateInSpec> specs, final MutateInOptions.Built opts) {
    notNullOrEmpty(id, "Id", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    notNullOrEmpty(specs, "MutateInSpecs", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    if (specs.isEmpty()) {
      throw SubdocMutateRequest.errIfNoCommands(ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    } else if (specs.size() > SubdocMutateRequest.SUBDOC_MAX_FIELDS) {
      throw SubdocMutateRequest.errIfTooManyCommands(ReducedKeyValueErrorContext.create(id, collectionIdentifier));
    } else {
      Duration timeout = decideKvTimeout(opts, environment.timeoutConfig());
      RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());
      JsonSerializer serializer = opts.serializer() == null ? environment.jsonSerializer() : opts.serializer();

      final InternalSpan span = environment
        .requestTracer()
        .internalSpan(SubdocMutateRequest.OPERATION_NAME, opts.parentSpan().orElse(null));

      ArrayList<SubdocMutateRequest.Command> commands = new ArrayList<>(specs.size());

      long start = System.nanoTime();
      try {
        span.startPayloadEncoding();
        for (int i = 0; i < specs.size(); i++) {
          MutateInSpec spec = specs.get(i);
          commands.add(spec.encode(serializer, i));
        }
      } finally {
        span.stopPayloadEncoding();
      }
      long end = System.nanoTime();

      // xattrs come first
      commands.sort(Comparator.comparing(v -> !v.xattr()));

      SubdocMutateRequest request = new SubdocMutateRequest(timeout, coreContext, collectionIdentifier, retryStrategy, id,
        opts.storeSemantics() == StoreSemantics.INSERT, opts.storeSemantics() == StoreSemantics.UPSERT,
        opts.accessDeleted(), opts.createAsDeleted(), commands, opts.expiry().getSeconds(), opts.cas(),
        opts.durabilityLevel(), span
      );
      request.context()
        .clientContext(opts.clientContext())
        .encodeLatency(end - start);
      return request;
    }
  }

  /**
   * Helper method to decide if the user timeout, the kv timeout or the durable kv timeout should be used.
   *
   * @param opts the built opts from the command.
   * @param config the env timeout config to use if not overridden by the user.
   * @return the timeout to use for the op.
   */
  @SuppressWarnings("unchecked")
  static Duration decideKvTimeout(CommonDurabilityOptions.BuiltCommonDurabilityOptions opts, TimeoutConfig config) {
    Optional<Duration> userTimeout = opts.timeout();
    if (userTimeout.isPresent()) {
      return userTimeout.get();
    }

    boolean syncDurability = opts.durabilityLevel().isPresent() && (
      opts.durabilityLevel().get() == DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE
      || opts.durabilityLevel().get() == DurabilityLevel.PERSIST_TO_MAJORITY);
    boolean pollDurability = opts.persistTo() != PersistTo.NONE;

    if (syncDurability || pollDurability) {
      return config.kvDurableTimeout();
    } else {
      return config.kvTimeout();
    }
  }

  CollectionIdentifier collectionIdentifier() {
    return collectionIdentifier;
  }

}
