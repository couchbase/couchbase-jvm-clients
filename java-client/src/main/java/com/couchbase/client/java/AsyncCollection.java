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
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.msg.kv.*;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.util.UnsignedLEB128;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.kv.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;
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
   * Holds the collection id in an encoded format.
   */
  private final byte[] collectionId;

  /**
   * Holds the async binary collection object.
   */
  private final AsyncBinaryCollection asyncBinaryCollection;

  /**
   * Creates a new {@link AsyncCollection}.
   *
   * @param name the name of the collection.
   * @param id the id
   * @param core the core into which ops are dispatched.
   * @param environment the surrounding environment for config options.
   */
  public AsyncCollection(final String name, final long id, final String bucket,
                         final Core core, final ClusterEnvironment environment) {
    this.name = name;
    this.core = core;
    this.coreContext = core.context();
    this.environment = environment;
    this.bucket = bucket;
    this.collectionId = UnsignedLEB128.encode(id);
    this.asyncBinaryCollection = new AsyncBinaryCollection(core, environment, bucket, collectionId);
  }

  /**
   * Provides access to the underlying {@link Core}.
   */
  @Stability.Internal
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
   * Returns the encoded collection id used for KV operations.
   */
  @Stability.Internal
  byte[] collectionId() {
    return collectionId;
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
   * <p>The {@link Optional} indicates if the document has been found or not. If the document
   * has not been found, an empty optional will be returned.</p>
   *
   * @param id the document id which is used to uniquely identify it.
   * @return a {@link CompletableFuture} indicating once loaded or failed.
   */
  public CompletableFuture<Optional<GetResult>> get(final String id) {
    return get(id, GetOptions.DEFAULT);
  }

  /**
   * Fetches a full document (or a projection of it) from a collection with custom options.
   *
   * <p>The {@link Optional} indicates if the document has been found or not. If the document
   * has not been found, an empty optional will be returned.</p>
   *
   * @param id the document id which is used to uniquely identify it.
   * @param options custom options to change the default behavior.
   * @return a {@link CompletableFuture} completing once loaded or failed.
   */
  public CompletableFuture<Optional<GetResult>> get(final String id, final GetOptions options) {
    notNull(options, "GetOptions");
    GetOptions.BuiltGetOptions opts = options.build();

    if (opts.projections() == null && !opts.withExpiration()) {
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
    GetOptions.BuiltGetOptions opts = options.build();

    Duration timeout = opts.timeout().orElse(environment.kvTimeout());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());
    return new GetRequest(id, collectionId, timeout, coreContext, bucket, retryStrategy);
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
    GetOptions.BuiltGetOptions opts = options.build();

    Duration timeout = opts.timeout().orElse(environment.kvTimeout());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());

    List<SubdocGetRequest.Command> commands = new ArrayList<>();

    if (opts.withExpiration()) {
      commands.add(new SubdocGetRequest.Command(
        SubdocCommandType.GET,
        EXPIRATION_MACRO,
        true
      ));
    }

    if (opts.projections() != null && !opts.projections().isEmpty()) {
      if (opts.projections().size() > 16) {
        throw new UnsupportedOperationException("Only a maximum of 16 fields can be "
          + "projected per request.");
      }

      commands.addAll(opts
        .projections()
        .stream()
        .filter(s -> s != null && !s.isEmpty())
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

    return new SubdocGetRequest(
      timeout, coreContext, bucket, retryStrategy, id, collectionId, (byte) 0, commands
    );
  }

  /**
   * Fetches a full document and write-locks it for the given duration with default options.
   *
   * <p>The {@link Optional} indicates if the document has been found or not. If the document
   * has not been found, an empty optional will be returned.</p>
   *
   * @param id the document id which is used to uniquely identify it.
   * @return a {@link CompletableFuture} completing once loaded or failed.
   */
  public CompletableFuture<Optional<GetResult>> getAndLock(final String id) {
    return getAndLock(id, GetAndLockOptions.DEFAULT);
  }

  /**
   * Fetches a full document and write-locks it for the given duration with custom options.
   *
   * <p>The {@link Optional} indicates if the document has been found or not. If the document
   * has not been found, an empty optional will be returned.</p>
   *
   * @param id the document id which is used to uniquely identify it.
   * @param options custom options to change the default behavior.
   * @return a {@link CompletableFuture} completing once loaded or failed.
   */
  public CompletableFuture<Optional<GetResult>> getAndLock(final String id,
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
    GetAndLockOptions.BuiltGetAndLockOptions opts = options.build();

    Duration timeout = opts.timeout().orElse(environment.kvTimeout());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());

    Duration lockFor = options.lockFor() == null ? Duration.ofSeconds(30) : options.lockFor();
    return new GetAndLockRequest(
      id, collectionId, timeout, coreContext, bucket, retryStrategy, lockFor
    );
  }

  /**
   * Fetches a full document and resets its expiration time to the value provided with default
   * options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param expiration the new expiration time for the document.
   * @return a {@link CompletableFuture} completing once loaded or failed.
   */
  public CompletableFuture<Optional<GetResult>> getAndTouch(final String id,
                                                            final Duration expiration) {
    return getAndTouch(id, expiration, GetAndTouchOptions.DEFAULT);
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
  public CompletableFuture<Optional<GetResult>> getAndTouch(final String id,
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
    GetAndTouchOptions.BuiltGetAndTouchOptions opts = options.build();

    Duration timeout = opts.timeout().orElse(environment.kvTimeout());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());
    return new GetAndTouchRequest(id, collectionId, timeout, coreContext,
      bucket, retryStrategy, expiration, opts.durabilityLevel());
  }

  /**
   * Reads from all available replicas and the active node and returns the results as a list
   * of futures that might complete or fail.
   *
   * @param id the document id.
   * @return a list of results from the active and the replica.
   */
  public List<CompletableFuture<Optional<GetResult>>> getFromReplica(final String id) {
    return getFromReplica(id, GetFromReplicaOptions.DEFAULT);
  }

  /**
   * Reads from replicas or the active node based on the options and returns the results as a list
   * of futures that might complete or fail.
   *
   * @param id the document id.
   * @return a list of results from the active and the replica.
   */
  public List<CompletableFuture<Optional<GetResult>>> getFromReplica(final String id,
                                                                     final GetFromReplicaOptions options) {
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
    GetFromReplicaOptions.BuiltGetFromReplicaOptions opts = options.build();

    Duration timeout = opts.timeout().orElse(environment.kvTimeout());
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
        requests.add(new GetRequest(id, collectionId, timeout, coreContext, bucket, retryStrategy));
        for (int i = 0; i < numReplicas; i++) {
          requests.add(new ReplicaGetRequest(
            id, collectionId, timeout, coreContext, bucket, retryStrategy, (short) (i + 1)
          ));
        }
        return requests.stream();
      } else {
        return Stream.of(new ReplicaGetRequest(
          id, collectionId, timeout, coreContext, bucket, retryStrategy,
          (short) opts.replicaMode().ordinal()
        ));
      }
    } else {
      throw new UnsupportedOperationException("Only couchbase buckets are supported "
        + "for replica get requests!");
    }
  }

  /**
   * Checks if the given document ID exists on the active partition with default options.
   *
   * @param id the document ID
   * @return a {@link CompletableFuture} completing once loaded or failed.
   */
  public CompletableFuture<Optional<ExistsResult>> exists(final String id) {
    return exists(id, ExistsOptions.DEFAULT);
  }

  /**
   * Checks if the given document ID exists on the active partition with custom options.
   *
   * @param id the document ID
   * @param options to modify the default behavior
   * @return a {@link CompletableFuture} completing once loaded or failed.
   */
  public CompletableFuture<Optional<ExistsResult>> exists(final String id,
                                                          final ExistsOptions options) {
    return ExistsAccessor.exists(core, id, existsRequest(id, options));
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
    ExistsOptions.BuiltExistsOptions opts = options.build();

    Duration timeout = opts.timeout().orElse(environment.kvTimeout());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());
    return new ObserveViaCasRequest(timeout, coreContext, bucket,
      retryStrategy, id, collectionId);
  }


  /**
   * Removes a Document from a collection with default options.
   *
   * @param id the id of the document to remove.
   * @return a {@link CompletableFuture} completing once removed or failed.
   */
  public CompletableFuture<MutationResult> remove(final String id) {
    return remove(id, RemoveOptions.DEFAULT);
  }

  /**
   * Removes a Document from a collection with custom options.
   *
   * @param id the id of the document to remove.
   * @param options custom options to change the default behavior.
   * @return a {@link CompletableFuture} completing once removed or failed.
   */
  public CompletableFuture<MutationResult> remove(final String id, final RemoveOptions options) {
    return RemoveAccessor.remove(core, removeRequest(id, options));
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
    RemoveOptions.BuiltRemoveOptions opts = options.build();

    Duration timeout = opts.timeout().orElse(environment.kvTimeout());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());
    return new RemoveRequest(id, collectionId, opts.cas(), timeout,
      coreContext, bucket, retryStrategy, opts.durabilityLevel());
  }

  /**
   * Inserts a full document which does not exist yet with default options.
   *
   * @param id the document id to insert.
   * @param content the document content to insert.
   * @return a {@link CompletableFuture} completing once inserted or failed.
   */
  public CompletableFuture<MutationResult> insert(final String id, Object content) {
    return insert(id, content, InsertOptions.DEFAULT);
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
    return InsertAccessor.insert(core, insertRequest(id, content, options));
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
    InsertOptions.BuiltInsertOptions opts = options.build();

    EncodedDocument encoded = opts.encoder().encode(content);
    Duration timeout = opts.timeout().orElse(environment.kvTimeout());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());

    return new InsertRequest(id, collectionId, encoded.content(), opts.expiry().getSeconds(),
      encoded.flags(), timeout, coreContext, bucket, retryStrategy, opts.durabilityLevel());
  }

  /**
   * Upserts a full document which might or might not exist yet with default options.
   *
   * @param id the document id to upsert.
   * @param content the document content to upsert.
   * @return a {@link CompletableFuture} completing once upserted or failed.
   */
  public CompletableFuture<MutationResult> upsert(final String id, Object content) {
    return upsert(id, content, UpsertOptions.DEFAULT);
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
    return UpsertAccessor.upsert(core, upsertRequest(id, content, options));
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
    UpsertOptions.BuiltUpsertOptions opts = options.build();

    EncodedDocument encoded = opts.encoder().encode(content);
    Duration timeout = opts.timeout().orElse(environment.kvTimeout());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());
    return new UpsertRequest(id, collectionId, encoded.content(), opts.expiry().getSeconds(),
      encoded.flags(), timeout, coreContext, bucket, retryStrategy, opts.durabilityLevel());
  }

  /**
   * Replaces a full document which already exists with default options.
   *
   * @param id the document id to replace.
   * @param content the document content to replace.
   * @return a {@link CompletableFuture} completing once replaced or failed.
   */
  public CompletableFuture<MutationResult> replace(final String id, Object content) {
    return replace(id, content, ReplaceOptions.DEFAULT);
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
    return ReplaceAccessor.replace(core, replaceRequest(id, content, options));
  }

  /**
   * Helper method to generate the replace request.
   *
   * @param id the document id to replace.
   * @param content the document content to replace.
   * @param options custom options to customize the replace behavior.
   * @return the replace request.
   */
  ReplaceRequest replaceRequest(final String id, final Object content, final ReplaceOptions options) {
    notNullOrEmpty(id, "Id");
    notNull(content, "Content");
    notNull(options, "ReplaceOptions");
    ReplaceOptions.BuiltReplaceOptions opts = options.build();

    EncodedDocument encoded = opts.encoder().encode(content);
    Duration timeout = opts.timeout().orElse(environment.kvTimeout());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());

    return new ReplaceRequest(id, collectionId, encoded.content(), opts.expiry().getSeconds(),
      encoded.flags(), timeout, opts.cas(), coreContext, bucket, retryStrategy, opts.durabilityLevel());
  }

  public CompletableFuture<MutationResult> touch(final String id, final Duration expiry) {
    return touch(id, expiry, TouchOptions.DEFAULT);
  }

  public CompletableFuture<MutationResult> touch(final String id, final Duration expiry,
                                                 final TouchOptions options) {
    return TouchAccessor.touch(core, touchRequest(id, expiry, options));
  }

  TouchRequest touchRequest(final String id, final Duration expiry, final TouchOptions options) {
    notNullOrEmpty(id, "Id");
    notNull(options, "TouchOptions");
    TouchOptions.BuiltTouchOptions opts = options.build();

    Duration timeout = opts.timeout().orElse(environment.kvTimeout());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());
    return new TouchRequest(timeout, coreContext, bucket, retryStrategy, id, collectionId,
      expiry.getSeconds(), opts.durabilityLevel());
  }

  public CompletableFuture<Void> unlock(final String id, final long cas) {
    return unlock(id, cas, UnlockOptions.DEFAULT);
  }

  public CompletableFuture<Void> unlock(final String id, final long cas, final UnlockOptions options) {
    return UnlockAccessor.unlock(core, unlockRequest(id, cas, options));
  }

  UnlockRequest unlockRequest(final String id, final long cas, final UnlockOptions options) {
    notNullOrEmpty(id, "Id");
    notNull(options, "UnlockOptions");
    UnlockOptions.BuiltUnlockoptions opts = options.build();

    Duration timeout = opts.timeout().orElse(environment.kvTimeout());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());
    return new UnlockRequest(timeout, coreContext, bucket, retryStrategy, id, collectionId, cas);
  }

  public CompletableFuture<Optional<LookupInResult>> lookupIn(final String id, final LookupInOps spec) {
    return lookupIn(id, spec, LookupInOptions.DEFAULT);
  }

  public CompletableFuture<Optional<LookupInResult>> lookupIn(final String id, final LookupInOps spec,
                                                              final LookupInOptions options) {
    return LookupInAccessor.lookupInAccessor(core, id, lookupInRequest(id, spec, options));
  }

  SubdocGetRequest lookupInRequest(final String id, final LookupInOps spec,
                                    final LookupInOptions options) {
    notNullOrEmpty(id, "Id");
    notNull(spec, "LookupInOps");
    notNull(options, "LookupInOptions");
    LookupInOptions.BuiltLookupInOptions opts = options.build();

    Duration timeout = opts.timeout().orElse(environment.kvTimeout());
    RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());
    return new SubdocGetRequest(timeout, coreContext, bucket, retryStrategy, id, collectionId,
      (byte) 0, spec.commands());
  }

  /**
   * Performs mutations to document fragments with default options.
   *
   * @param id the outer document ID.
   * @param spec the spec which specifies the type of mutations to perform.
   * @return the {@link MutateInResult} once the mutation has been performed or failed.
   */
  public CompletableFuture<MutateInResult> mutateIn(final String id, final MutateInOps spec) {
    return mutateIn(id, spec, MutateInOptions.DEFAULT);
  }

  /**
   * Performs mutations to document fragments with custom options.
   *
   * @param id the outer document ID.
   * @param spec the spec which specifies the type of mutations to perform.
   * @param options custom options to modify the mutation options.
   * @return the {@link MutateInResult} once the mutation has been performed or failed.
   */
  public CompletableFuture<MutateInResult> mutateIn(final String id, final MutateInOps spec,
                                                    final MutateInOptions options) {
    return MutateInAccessor.mutateIn(core, mutateInRequest(id, spec, options));
  }

  SubdocMutateRequest mutateInRequest(final String id, final MutateInOps spec,
                                      final MutateInOptions options) {
    if (spec.commands().isEmpty()) {
      throw SubdocMutateRequest.errIfNoCommands();
    }
    else if (spec.commands().size() > SubdocMutateRequest.SUBDOC_MAX_FIELDS) {
      throw SubdocMutateRequest.errIfTooManyCommands();
    }
    else {
      notNullOrEmpty(id, "Id");
      notNull(spec, "MutateInSpec");
      notNull(options, "MutateInOptions");
      MutateInOptions.BuiltMutateInOptions opts = options.build();

      Duration timeout = opts.timeout().orElse(environment.kvTimeout());
      RetryStrategy retryStrategy = opts.retryStrategy().orElse(environment.retryStrategy());
      return new SubdocMutateRequest(timeout, coreContext, bucket, retryStrategy, id, collectionId,
              opts.insertDocument(), spec.commands(), opts.expiry().getSeconds(), opts.durabilityLevel());
    }
  }

}
