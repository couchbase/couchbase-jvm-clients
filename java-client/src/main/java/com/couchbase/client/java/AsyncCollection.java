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
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.error.CommonExceptions;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.kv.GetAndLockRequest;
import com.couchbase.client.core.msg.kv.GetAndTouchRequest;
import com.couchbase.client.core.msg.kv.GetRequest;
import com.couchbase.client.core.msg.kv.InsertRequest;
import com.couchbase.client.core.msg.kv.ObserveViaCasRequest;
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
import com.couchbase.client.java.codec.DataFormat;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.kv.ExistsAccessor;
import com.couchbase.client.java.kv.ExistsOptions;
import com.couchbase.client.java.kv.ExistsResult;
import com.couchbase.client.java.kv.GetAccessor;
import com.couchbase.client.java.kv.GetAndLockOptions;
import com.couchbase.client.java.kv.GetAndTouchOptions;
import com.couchbase.client.java.kv.GetFromReplicaOptions;
import com.couchbase.client.java.kv.GetOptions;
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
import com.couchbase.client.java.kv.RemoveAccessor;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.ReplaceAccessor;
import com.couchbase.client.java.kv.ReplaceOptions;
import com.couchbase.client.java.kv.ReplicaMode;
import com.couchbase.client.java.kv.TouchAccessor;
import com.couchbase.client.java.kv.TouchOptions;
import com.couchbase.client.java.kv.UnlockAccessor;
import com.couchbase.client.java.kv.UnlockOptions;
import com.couchbase.client.java.kv.UpsertAccessor;
import com.couchbase.client.java.kv.UpsertOptions;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;
import static com.couchbase.client.java.ReactiveCollection.DEFAULT_EXISTS_OPTIONS;
import static com.couchbase.client.java.ReactiveCollection.DEFAULT_GET_AND_LOCK_OPTIONS;
import static com.couchbase.client.java.ReactiveCollection.DEFAULT_GET_AND_TOUCH_OPTIONS;
import static com.couchbase.client.java.ReactiveCollection.DEFAULT_GET_FROM_REPLICA_OPTIONS;
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
    notNull(options, "GetOptions");
    GetOptions.Built opts = options.build();

    if (opts.projections().isEmpty() && !opts.withExpiration()) {
      return GetAccessor.get(core, id, fullGetRequest(id, options));
    } else {
      return GetAccessor.subdocGet(core, id, subdocGetRequest(id, options));
    }
  }

  /**
   * Helper method to create a get request for a full doc fetch.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param options custom options to change the default behavior.
   * @return the get request.
   */
  @Stability.Internal
  GetRequest fullGetRequest(final String id, final GetOptions options) {
    notNullOrEmpty(id, "Id");
    notNull(options, "GetOptions");
    GetOptions.Built opts = options.build();

    Duration timeout = opts.timeout().orElse(environment.timeoutConfig().kvTimeout());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());
    GetRequest request = new GetRequest(id, timeout, coreContext, collectionIdentifier, retryStrategy);
    request.context().clientContext(opts.clientContext());
    return request;
  }

  /**
   * Helper method to create a get request for a subdoc fetch.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param options custom options to change the default behavior.
   * @return the subdoc get request.
   */
  @Stability.Internal
  SubdocGetRequest subdocGetRequest(final String id, final GetOptions options) {
    notNullOrEmpty(id, "Id");
    notNull(options, "GetOptions");
    GetOptions.Built opts = options.build();

    Duration timeout = opts.timeout().orElse(environment.timeoutConfig().kvTimeout());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());

    List<SubdocGetRequest.Command> commands = new ArrayList<>();

    if (opts.withExpiration()) {
      commands.add(new SubdocGetRequest.Command(
        SubdocCommandType.GET,
        EXPIRATION_MACRO,
        true
      ));
    }

    if (!opts.projections().isEmpty()) {
      if (opts.projections().size() > 16) {
        throw new UnsupportedOperationException("Only a maximum of 16 fields can be "
          + "projected per request.");
      }

      commands.addAll(opts
        .projections()
        .stream()
        .map(s -> new SubdocGetRequest.Command(SubdocCommandType.GET, s, false))
        .collect(Collectors.toList())
      );
    } else {
      commands.add(new SubdocGetRequest.Command(
        SubdocCommandType.GET_DOC,
        "",
        false
      ));
    }

    SubdocGetRequest request = new SubdocGetRequest(
      timeout, coreContext, collectionIdentifier, retryStrategy, id, (byte) 0, commands
    );
    request.context().clientContext(opts.clientContext());
    return request;
  }

  /**
   * Fetches a full document and write-locks it for the given duration with default options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @return a {@link CompletableFuture} completing once loaded or failed.
   */
  public CompletableFuture<GetResult> getAndLock(final String id) {
    return getAndLock(id, DEFAULT_GET_AND_LOCK_OPTIONS);
  }

  /**
   * Fetches a full document and write-locks it for the given duration with custom options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param options custom options to change the default behavior.
   * @return a {@link CompletableFuture} completing once loaded or failed.
   */
  public CompletableFuture<GetResult> getAndLock(final String id,
                                                           final GetAndLockOptions options) {
    return GetAccessor.getAndLock(core, id, getAndLockRequest(id, options));
  }

  /**
   * Helper method to create the get and lock request.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param options custom options to change the default behavior.
   * @return the get and lock request.
   */
  @Stability.Internal
  GetAndLockRequest getAndLockRequest(final String id, final GetAndLockOptions options) {
    notNullOrEmpty(id, "Id");
    notNull(options, "GetAndLockOptions");
    GetAndLockOptions.Built opts = options.build();

    Duration timeout = opts.timeout().orElse(environment.timeoutConfig().kvTimeout());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());

    Duration lockFor = opts.lockFor() == null ? Duration.ofSeconds(30) : opts.lockFor();
    GetAndLockRequest request = new GetAndLockRequest(
      id, timeout, coreContext, collectionIdentifier, retryStrategy, lockFor
    );
    request.context().clientContext(opts.clientContext());
    return request;
  }

  /**
   * Fetches a full document and resets its expiration time to the value provided with default
   * options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param expiration the new expiration time for the document.
   * @return a {@link CompletableFuture} completing once loaded or failed.
   */
  public CompletableFuture<GetResult> getAndTouch(final String id,
                                                            final Duration expiration) {
    return getAndTouch(id, expiration, DEFAULT_GET_AND_TOUCH_OPTIONS);
  }

  /**
   * Fetches a full document and resets its expiration time to the value provided with custom
   * options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param expiration the new expiration time for the document.
   * @param options custom options to change the default behavior.
   * @return a {@link CompletableFuture} completing once loaded or failed.
   */
  public CompletableFuture<GetResult> getAndTouch(final String id,
                                                            final Duration expiration,
                                                            final GetAndTouchOptions options) {
    return GetAccessor.getAndTouch(core, id, getAndTouchRequest(id, expiration, options));
  }

  /**
   * Helper method for get and touch requests.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param expiration the new expiration time for the document.
   * @param options custom options to change the default behavior.
   * @return the get and touch request.
   */
  @Stability.Internal
  GetAndTouchRequest getAndTouchRequest(final String id, final Duration expiration,
                                        final GetAndTouchOptions options) {
    notNullOrEmpty(id, "Id");
    notNull(expiration, "Expiration");
    notNull(options, "GetAndTouchOptions");
    GetAndTouchOptions.Built opts = options.build();

    Duration timeout = opts.timeout().orElse(environment.timeoutConfig().kvTimeout());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());
    GetAndTouchRequest request = new GetAndTouchRequest(id, timeout, coreContext,
      collectionIdentifier, retryStrategy, expiration);
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
  public List<CompletableFuture<GetResult>> getFromReplica(final String id) {
    return getFromReplica(id, DEFAULT_GET_FROM_REPLICA_OPTIONS);
  }

  /**
   * Reads from replicas or the active node based on the options and returns the results as a list
   * of futures that might complete or fail.
   *
   * @param id the document id.
   * @return a list of results from the active and the replica.
   */
  // TODO sync with RFC changes
  public List<CompletableFuture<GetResult>> getFromReplica(final String id, final GetFromReplicaOptions options) {
    return getFromReplicaRequests(id, options)
      .map(request -> GetAccessor.get(core, id, request))
      .collect(Collectors.toList());
  }

  /**
   * Helper method to assemble a stream of requests either to the active or to the replica.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param options custom options to change the default behavior.
   * @return a stream of requests.
   */
  Stream<GetRequest> getFromReplicaRequests(final String id,
                                            final GetFromReplicaOptions options) {
    notNullOrEmpty(id, "Id");
    notNull(options, "GetFromReplicaOptions");
    GetFromReplicaOptions.Built opts = options.build();

    Duration timeout = opts.timeout().orElse(environment.timeoutConfig().kvTimeout());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());

    BucketConfig config = core.clusterConfig().bucketConfig(bucket);
    if (config == null) {
      throw new CouchbaseException("No bucket config found, " +
        "this is a bug and not supposed to happen. Please report!");
    }

    if (config instanceof CouchbaseBucketConfig) {
      if (opts.replicaMode() == ReplicaMode.ALL) {
        int numReplicas = ((CouchbaseBucketConfig) config).numberOfReplicas();
        List<GetRequest> requests = new ArrayList<>(numReplicas + 1);

        GetRequest activeRequest = new GetRequest(id, timeout, coreContext, collectionIdentifier, retryStrategy);
        activeRequest.context().clientContext(opts.clientContext());
        requests.add(activeRequest);

        for (int i = 0; i < numReplicas; i++) {
          ReplicaGetRequest replicaRequest = new ReplicaGetRequest(
            id, timeout, coreContext, collectionIdentifier, retryStrategy, (short) (i + 1)
          );
          replicaRequest.context().clientContext(opts.clientContext());
          requests.add(replicaRequest);
        }
        return requests.stream();
      } else {
        ReplicaGetRequest replicaRequest = new ReplicaGetRequest(
          id, timeout, coreContext, collectionIdentifier, retryStrategy,
          (short) opts.replicaMode().ordinal()
        );
        replicaRequest.context().clientContext(opts.clientContext());
        return Stream.of(replicaRequest);
      }
    } else {
      throw CommonExceptions.getFromReplicaNotCouchbaseBucket();
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
  ObserveViaCasRequest existsRequest(final String id, final ExistsOptions options) {
    notNullOrEmpty(id, "Id");
    notNull(options, "ExistsOptions");
    ExistsOptions.Built opts = options.build();

    Duration timeout = opts.timeout().orElse(environment.timeoutConfig().kvTimeout());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());
    ObserveViaCasRequest request = new ObserveViaCasRequest(timeout, coreContext, collectionIdentifier,
      retryStrategy, id, true, 0);
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
    RemoveOptions.Built opts = options.build();
    return RemoveAccessor.remove(core, removeRequest(id, options), id, opts.persistTo(),
      opts.replicateTo());
  }

  /**
   * Helper method to create the remove request.
   *
   * @param id the id of the document to remove.
   * @param options custom options to change the default behavior.
   * @return the remove request.
   */
  RemoveRequest removeRequest(final String id, final RemoveOptions options) {
    notNullOrEmpty(id, "Id");
    notNull(options, "RemoveOptions");
    RemoveOptions.Built opts = options.build();

    Duration timeout = opts.timeout().orElse(environment.timeoutConfig().kvTimeout());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());
    RemoveRequest request = new RemoveRequest(id, opts.cas(), timeout,
      coreContext, collectionIdentifier, retryStrategy, opts.durabilityLevel());
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
  public CompletableFuture<MutationResult> insert(final String id, Object content,
                                                  final InsertOptions options) {
    InsertOptions.Built opts = options.build();
    return InsertAccessor.insert(core, insertRequest(id, content, options), id, opts.persistTo(),
      opts.replicateTo());
  }

  /**
   * Helper method to generate the insert request.
   *
   * @param id the document id to insert.
   * @param content the document content to insert.
   * @param options custom options to customize the insert behavior.
   * @return the insert request.
   */
  InsertRequest insertRequest(final String id, final Object content, final InsertOptions options) {
    notNullOrEmpty(id, "Id");
    notNull(content, "Content");
    notNull(options, "InsertOptions");
    InsertOptions.Built opts = options.build();

    Duration timeout = opts.timeout().orElse(environment.timeoutConfig().kvTimeout());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());
    DataFormat dataFormat = opts.dataFormat();

    InsertRequest request = new InsertRequest(id, opts.transcoder().encode(content, dataFormat),
      opts.expiry().getSeconds(), dataFormat.commonFlag(), timeout, coreContext, collectionIdentifier,
      retryStrategy, opts.durabilityLevel());
    request.context().clientContext(opts.clientContext());
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
  public CompletableFuture<MutationResult> upsert(final String id, Object content,
                                                  final UpsertOptions options) {
    UpsertOptions.Built opts = options.build();
    return UpsertAccessor.upsert(
      core,
      upsertRequest(id, content, options),
      id,
      opts.persistTo(),
      opts.replicateTo()
    );
  }

  /**
   * Helper method to generate the upsert request.
   *
   * @param id the document id to upsert.
   * @param content the document content to upsert.
   * @param options custom options to customize the upsert behavior.
   * @return the upsert request.
   */
  UpsertRequest upsertRequest(final String id, final Object content, final UpsertOptions options) {
    notNullOrEmpty(id, "Id");
    notNull(content, "Content");
    notNull(options, "UpsertOptions");
    UpsertOptions.Built opts = options.build();

    Duration timeout = opts.timeout().orElse(environment.timeoutConfig().kvTimeout());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());
    DataFormat dataFormat = opts.dataFormat();

    UpsertRequest request = new UpsertRequest(id, opts.transcoder().encode(content, dataFormat),
      opts.expiry().getSeconds(), dataFormat.commonFlag(), timeout, coreContext, collectionIdentifier,
      retryStrategy, opts.durabilityLevel());
    request.context().clientContext(opts.clientContext());
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
  public CompletableFuture<MutationResult> replace(final String id, Object content,
                                                   final ReplaceOptions options) {
    ReplaceOptions.Built opts = options.build();
    return ReplaceAccessor.replace(
      core,
      replaceRequest(id, content, options),
      id,
      opts.persistTo(),
      opts.replicateTo()
    );
  }

  /**
   * Helper method to generate the replace request.
   *
   * @param id the document id to replace.
   * @param content the document content to replace.
   * @param options custom options to customize the replace behavior.
   * @return the replace request.
   */
  ReplaceRequest replaceRequest(final String id, final Object content,
                                final ReplaceOptions options) {
    notNullOrEmpty(id, "Id");
    notNull(content, "Content");
    notNull(options, "ReplaceOptions");
    ReplaceOptions.Built opts = options.build();

    Duration timeout = opts.timeout().orElse(environment.timeoutConfig().kvTimeout());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());
    DataFormat dataFormat = opts.dataFormat();

    ReplaceRequest request = new ReplaceRequest(id,  opts.transcoder().encode(content, dataFormat),
      opts.expiry().getSeconds(), dataFormat.commonFlag(), timeout, opts.cas(), coreContext,
      collectionIdentifier, retryStrategy, opts.durabilityLevel());
    request.context().clientContext(opts.clientContext());
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
  public CompletableFuture<MutationResult> touch(final String id, final Duration expiry,
                                                 final TouchOptions options) {
    TouchOptions.Built opts = options.build();
    return TouchAccessor.touch(
      core,
      touchRequest(id, expiry, options),
      id,
      opts.persistTo(),
      opts.replicateTo()
    );
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
    notNullOrEmpty(id, "Id");
    notNull(options, "TouchOptions");
    TouchOptions.Built opts = options.build();

    Duration timeout = opts.timeout().orElse(environment.timeoutConfig().kvTimeout());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());
    TouchRequest request = new TouchRequest(timeout, coreContext, collectionIdentifier, retryStrategy, id,
      expiry.getSeconds(), opts.durabilityLevel());
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
  public CompletableFuture<Void> unlock(final String id, final long cas,
                                        final UnlockOptions options) {
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
    notNullOrEmpty(id, "Id");
    notNull(options, "UnlockOptions");
    UnlockOptions.Built opts = options.build();

    Duration timeout = opts.timeout().orElse(environment.timeoutConfig().kvTimeout());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());
    UnlockRequest request = new UnlockRequest(timeout, coreContext, collectionIdentifier, retryStrategy, id, cas);
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
  public CompletableFuture<LookupInResult> lookupIn(final String id,
                                                              final List<LookupInSpec> specs) {
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
  public CompletableFuture<LookupInResult> lookupIn(final String id,
                                                              final List<LookupInSpec> specs,
                                                              final LookupInOptions options) {
    return LookupInAccessor.lookupInAccessor(id, core, lookupInRequest(id, specs, options), options.build().withExpiration());
  }

  /**
   * Helper method to create the underlying lookup subdoc request.
   *
   * @param id the outer document ID.
   * @param specs the spec which specifies the type of lookups to perform.
   * @param options custom options to modify the lookup options.
   * @return the subdoc lookup request.
   */
  SubdocGetRequest lookupInRequest(final String id, final List<LookupInSpec> specs,
                                   final LookupInOptions options) {
    notNullOrEmpty(id, "Id");
    notNullOrEmpty(specs, "LookupInSpecs");
    notNull(options, "LookupInOptions");
    LookupInOptions.Built opts = options.build();

    ArrayList<SubdocGetRequest.Command> commands = new ArrayList<>();

    if (options.build().withExpiration()) {
      // Xattr commands have to go first
      commands.add(new SubdocGetRequest.Command(SubdocCommandType.GET, EXPIRATION_MACRO, true));
    }

    for (LookupInSpec spec : specs) {
      commands.add(spec.export());
    }

    Duration timeout = opts.timeout().orElse(environment.timeoutConfig().kvTimeout());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());
    SubdocGetRequest request = new SubdocGetRequest(timeout, coreContext, collectionIdentifier, retryStrategy, id,
      (byte) 0, commands);
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
    MutateInOptions.Built opts = options.build();
    return MutateInAccessor.mutateIn(
      core,
      mutateInRequest(id, specs, options),
      id,
      opts.persistTo(),
      opts.replicateTo(),
      opts.insertDocument()
    );
  }

  /**
   * Helper method to create the underlying subdoc mutate request.
   *
   * @param id the outer document ID.
   * @param specs the spec which specifies the type of mutations to perform.
   * @param options custom options to modify the mutation options.
   * @return the subdoc mutate request.
   */
  SubdocMutateRequest mutateInRequest(final String id, final List<MutateInSpec> specs,
                                      final MutateInOptions options) {
    if (specs.isEmpty()) {
      throw SubdocMutateRequest.errIfNoCommands();
    } else if (specs.size() > SubdocMutateRequest.SUBDOC_MAX_FIELDS) {
      throw SubdocMutateRequest.errIfTooManyCommands();
    } else {
      notNullOrEmpty(id, "Id");
      notNull(specs, "MutateInSpecs");
      notNull(options, "MutateInOptions");
      MutateInOptions.Built opts = options.build();

      Duration timeout = opts.timeout().orElse(environment.timeoutConfig().kvTimeout());
      RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());

      List<SubdocMutateRequest.Command> commands = specs
        .stream()
        .map(v -> v.encode())
        .collect(Collectors.toList());

      SubdocMutateRequest request = new SubdocMutateRequest(timeout, coreContext, collectionIdentifier, retryStrategy, id,
        opts.insertDocument(), opts.upsertDocument(),
        commands, opts.expiry().getSeconds(), opts.cas(),
        opts.durabilityLevel()
      );
      request.context().clientContext(opts.clientContext());
      return request;
    }
  }

}
