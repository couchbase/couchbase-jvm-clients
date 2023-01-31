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
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.kv.CoreKvOps;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.core.error.context.ReducedKeyValueErrorContext;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.codec.Transcoder;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.kv.ExistsOptions;
import com.couchbase.client.java.kv.ExistsResult;
import com.couchbase.client.java.kv.Expiry;
import com.couchbase.client.java.kv.GetAllReplicasOptions;
import com.couchbase.client.java.kv.GetAndLockOptions;
import com.couchbase.client.java.kv.GetAndTouchOptions;
import com.couchbase.client.java.kv.GetAnyReplicaOptions;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.GetReplicaResult;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.LookupInOptions;
import com.couchbase.client.java.kv.LookupInResult;
import com.couchbase.client.java.kv.LookupInSpec;
import com.couchbase.client.java.kv.MutateInOptions;
import com.couchbase.client.java.kv.MutateInResult;
import com.couchbase.client.java.kv.MutateInSpec;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.ReplaceOptions;
import com.couchbase.client.java.kv.ScanOptions;
import com.couchbase.client.java.kv.ScanResult;
import com.couchbase.client.java.kv.ScanType;
import com.couchbase.client.java.kv.TouchOptions;
import com.couchbase.client.java.kv.UnlockOptions;
import com.couchbase.client.java.kv.UpsertOptions;
import com.couchbase.client.java.manager.query.ReactiveCollectionQueryIndexManager;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static com.couchbase.client.core.util.CbCollections.transform;
import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.java.kv.ExistsOptions.existsOptions;
import static com.couchbase.client.java.kv.GetAllReplicasOptions.getAllReplicasOptions;
import static com.couchbase.client.java.kv.GetAndLockOptions.getAndLockOptions;
import static com.couchbase.client.java.kv.GetAndTouchOptions.getAndTouchOptions;
import static com.couchbase.client.java.kv.GetAnyReplicaOptions.getAnyReplicaOptions;
import static com.couchbase.client.java.kv.GetOptions.getOptions;
import static com.couchbase.client.java.kv.InsertOptions.insertOptions;
import static com.couchbase.client.java.kv.LookupInOptions.lookupInOptions;
import static com.couchbase.client.java.kv.MutateInOptions.mutateInOptions;
import static com.couchbase.client.java.kv.RemoveOptions.removeOptions;
import static com.couchbase.client.java.kv.ReplaceOptions.replaceOptions;
import static com.couchbase.client.java.kv.TouchOptions.touchOptions;
import static com.couchbase.client.java.kv.UnlockOptions.unlockOptions;
import static com.couchbase.client.java.kv.UpsertOptions.upsertOptions;

/**
 * The {@link ReactiveCollection} provides sophisticated asynchronous access to all collection APIs.
 *
 * <p>This API provides more sophisticated async controls over the {@link AsyncCollection}, but
 * it also comes with a little more overhead. For most use cases we recommend using this API
 * over the other one, unless you really need that last drop of performance and can live with the
 * significantly reduced functionality (in terms of the richness of operators). For example, this
 * {@link ReactiveCollection} is built on top of the {@link AsyncCollection}.</p>
 *
 * @since 3.0.0
 */
public class ReactiveCollection {

  static final ExistsOptions DEFAULT_EXISTS_OPTIONS = existsOptions();
  static final GetAndLockOptions DEFAULT_GET_AND_LOCK_OPTIONS = getAndLockOptions();
  static final GetAndTouchOptions DEFAULT_GET_AND_TOUCH_OPTIONS = getAndTouchOptions();
  static final GetAllReplicasOptions DEFAULT_GET_ALL_REPLICAS_OPTIONS = getAllReplicasOptions();
  static final GetAnyReplicaOptions DEFAULT_GET_ANY_REPLICA_OPTIONS = getAnyReplicaOptions();
  static final GetOptions DEFAULT_GET_OPTIONS = getOptions();
  static final InsertOptions DEFAULT_INSERT_OPTIONS = insertOptions();
  static final LookupInOptions DEFAULT_LOOKUP_IN_OPTIONS = lookupInOptions();
  static final MutateInOptions DEFAULT_MUTATE_IN_OPTIONS = mutateInOptions();
  static final RemoveOptions DEFAULT_REMOVE_OPTIONS = removeOptions();
  static final ReplaceOptions DEFAULT_REPLACE_OPTIONS = replaceOptions();
  static final TouchOptions DEFAULT_TOUCH_OPTIONS = touchOptions();
  static final UnlockOptions DEFAULT_UNLOCK_OPTIONS = unlockOptions();
  static final UpsertOptions DEFAULT_UPSERT_OPTIONS = upsertOptions();

  /**
   * Holds the underlying async collection.
   */
  private final AsyncCollection asyncCollection;

  /**
   * Strategy for performing KV operations.
   */
  private final CoreKvOps kvOps;

  /**
   * Holds the related binary collection.
   */
  private final ReactiveBinaryCollection reactiveBinaryCollection;

  /**
   * Allows managing query indexes at the Collection level.
   */
  private final ReactiveCollectionQueryIndexManager queryIndexManager;

  ReactiveCollection(final AsyncCollection asyncCollection) {
    this.asyncCollection = asyncCollection;
    this.reactiveBinaryCollection = new ReactiveBinaryCollection(asyncCollection.binary());
    this.kvOps = asyncCollection.kvOps;
    this.queryIndexManager = new ReactiveCollectionQueryIndexManager(asyncCollection.queryIndexes());
  }

  /**
   * Provides access to the underlying {@link AsyncCollection}.
   *
   * @return returns the underlying {@link AsyncCollection}.
   */
  public AsyncCollection async() {
    return asyncCollection;
  }

  /**
   * Returns the name of this collection.
   */
  public String name() {
    return asyncCollection.name();
  }

  /**
   * Returns the name of the bucket associated with this collection.
   */
  public String bucketName() {
    return asyncCollection.bucketName();
  }

  /**
   * Returns the name of the scope associated with this collection.
   */
  public String scopeName() {
    return asyncCollection.scopeName();
  }

  /**
   * Provides access to the underlying {@link Core}.
   */
  @Stability.Volatile
  public Core core() {
    return asyncCollection.core();
  }

  /**
   * Provides access to the underlying {@link ClusterEnvironment}.
   */
  public ClusterEnvironment environment() {
    return asyncCollection.environment();
  }
  /**
   * Provides access to the binary APIs, not used for JSON documents.
   *
   * @return the {@link ReactiveBinaryCollection}.
   */
  public ReactiveBinaryCollection binary() {
    return reactiveBinaryCollection;
  }

  /**
   * Provides access to query index management at the ReactiveCollection level.
   */
  @Stability.Volatile
  public ReactiveCollectionQueryIndexManager queryIndexes() {
    return queryIndexManager;
  }

  /**
   * Fetches a Document from a collection with default options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @return a {@link Mono} indicating once loaded or failed.
   */
  public Mono<GetResult> get(final String id) {
    return get(id, DEFAULT_GET_OPTIONS);
  }

  /**
   * Fetches a Document from a collection with custom options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param options custom options to change the default behavior.
   * @return a {@link Mono} indicating once loaded or failed
   */
  public Mono<GetResult> get(final String id, final GetOptions options) {
    GetOptions.Built opts = options.build();
    Transcoder transcoder = opts.transcoder() == null ? environment().transcoder() : opts.transcoder();

    return kvOps.getReactive(opts, id, opts.projections(), opts.withExpiry())
      .map(it -> new GetResult(it, transcoder));
  }

  /**
   * Fetches a full document and write-locks it for the given duration with default options.
   * <p>
   * Note that the client does not enforce an upper limit on the {@link Duration} lockTime. The maximum lock time
   * by default on the server is 30 seconds. Any value larger than 30 seconds will be capped down by the server to
   * the default lock time, which is 15 seconds unless modified on the server side.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param lockTime how long to write-lock the document for (any duration > 30s will be capped to server default of 15s).
   * @return a {@link Mono} completing once loaded or failed.
   */
  public Mono<GetResult> getAndLock(final String id, final Duration lockTime) {
    return getAndLock(id, lockTime, DEFAULT_GET_AND_LOCK_OPTIONS);
  }

  /**
   * Fetches a full document and write-locks it for the given duration with custom options.
   * <p>
   * Note that the client does not enforce an upper limit on the {@link Duration} lockTime. The maximum lock time
   * by default on the server is 30 seconds. Any value larger than 30 seconds will be capped down by the server to
   * the default lock time, which is 15 seconds unless modified on the server side.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param lockTime how long to write-lock the document for (any duration > 30s will be capped to server default of 15s).
   * @param options custom options to change the default behavior.
   * @return a {@link Mono} completing once loaded or failed.
   */
  public Mono<GetResult> getAndLock(final String id, final Duration lockTime, final GetAndLockOptions options) {
    GetAndLockOptions.Built opts = options.build();
    Transcoder transcoder = opts.transcoder() == null ? environment().transcoder() : opts.transcoder();

    return kvOps.getAndLockReactive(opts, id, lockTime)
      .map(it -> new GetResult(it, transcoder));
  }

  /**
   * Fetches a full document and resets its expiration time to the value provided with default
   * options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param expiry the new expiration time for the document.
   * @return a {@link Mono} completing once loaded or failed.
   */
  public Mono<GetResult> getAndTouch(final String id, final Duration expiry) {
    return getAndTouch(id, expiry, DEFAULT_GET_AND_TOUCH_OPTIONS);
  }

  /**
   * Fetches a full document and resets its expiration time to the value provided with custom
   * options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param expiry the new expiration time for the document.
   * @param options custom options to change the default behavior.
   * @return a {@link Mono} completing once loaded or failed.
   */
  public Mono<GetResult> getAndTouch(final String id, final Duration expiry, final GetAndTouchOptions options) {
    GetAndTouchOptions.Built opts = options.build();
    Transcoder transcoder = opts.transcoder() == null ? environment().transcoder() : opts.transcoder();

    return kvOps.getAndTouchReactive(opts, id, Expiry.relative(expiry).encode())
      .map(it -> new GetResult(it, transcoder));
  }

  /**
   * Reads all available replicas, including the active, and returns the results as a flux.
   * <p>
   * Note that individual errors are ignored, so you can think of this API as a best effort
   * approach which explicitly emphasises availability over consistency.
   * <p>
   * If the read requests all fail, the flux emits nothing.
   *
   * @param id the document id.
   * @return a flux of results from all replicas
   */
  public Flux<GetReplicaResult> getAllReplicas(final String id) {
    return getAllReplicas(id, DEFAULT_GET_ALL_REPLICAS_OPTIONS);
  }

  /**
   * Reads all available replicas, including the active, and returns the results as a flux.
   * <p>
   * Note that individual errors are ignored, so you can think of this API as a best effort
   * approach which explicitly emphasises availability over consistency.
   * <p>
   * If the read requests all fail, the flux emits nothing.
   *
   * @param id the document id.
   * @param options the custom options.
   * @return a flux of results from all replicas
   */
  public Flux<GetReplicaResult> getAllReplicas(final String id, final GetAllReplicasOptions options) {
    notNull(options, "GetAllReplicasOptions", () -> ReducedKeyValueErrorContext.create(id, asyncCollection.collectionIdentifier()));
    GetAllReplicasOptions.Built opts = options.build();
    final Transcoder transcoder = Optional.ofNullable(opts.transcoder()).orElse(environment().transcoder());

    return kvOps.getAllReplicasReactive(opts, id)
      .map(response -> GetReplicaResult.from(response, transcoder));
  }

  /**
   * Reads all available replicas, and returns the first found.
   * <p>
   * If the read requests all fail, the mono emits nothing.
   *
   * @param id the document id.
   * @return a mono containing the first available replica.
   */
  public Mono<GetReplicaResult> getAnyReplica(final String id) {
    return getAnyReplica(id, DEFAULT_GET_ANY_REPLICA_OPTIONS);
  }

  /**
   * Reads all available replicas, and returns the first found.
   * <p>
   * If the read requests all fail, the mono emits nothing.
   *
   * @param id the document id.
   * @param options the custom options.
   * @return a mono containing the first available replica.
   */
  public Mono<GetReplicaResult> getAnyReplica(final String id, final GetAnyReplicaOptions options) {
    notNull(options, "GetAnyReplicaOptions", () -> ReducedKeyValueErrorContext.create(id, asyncCollection.collectionIdentifier()));
    GetAnyReplicaOptions.Built opts = options.build();
    final Transcoder transcoder = Optional.ofNullable(opts.transcoder()).orElse(environment().transcoder());

    return kvOps.getAnyReplicaReactive(opts, id)
      .map(response -> GetReplicaResult.from(response, transcoder));
  }

  /**
   * Checks if the given document ID exists on the active partition with default options.
   *
   * @param id the document ID
   * @return a {@link Mono} completing once loaded or failed.
   */
  public Mono<ExistsResult> exists(final String id) {
    return exists(id, DEFAULT_EXISTS_OPTIONS);
  }

  /**
   * Checks if the given document ID exists on the active partition with custom options.
   *
   * @param id the document ID
   * @param options to modify the default behavior
   * @return a {@link Mono} completing once loaded or failed.
   */
  public Mono<ExistsResult> exists(final String id, final ExistsOptions options) {
    ExistsOptions.Built opts = notNull(options, "ExistsOptions").build();
    return kvOps.existsReactive(opts, id)
      .map(ExistsResult::from);
  }

  /**
   * Removes a Document from a collection with default options.
   *
   * @param id the id of the document to remove.
   * @return a {@link Mono} completing once removed or failed.
   */
  public Mono<MutationResult> remove(final String id) {
    return remove(id, DEFAULT_REMOVE_OPTIONS);
  }

  /**
   * Removes a Document from a collection with custom options.
   *
   * @param id the id of the document to remove.
   * @param options custom options to change the default behavior.
   * @return a {@link Mono} completing once removed or failed.
   */
  public Mono<MutationResult> remove(final String id, final RemoveOptions options) {
    notNull(options, "RemoveOptions", () -> ReducedKeyValueErrorContext.create(id, asyncCollection.collectionIdentifier()));

    RemoveOptions.Built opts = options.build();
    return kvOps.removeReactive(
        opts,
        id,
        opts.cas(),
        opts.toCoreDurability()
      )
      .map(MutationResult::new);
  }

  /**
   * Inserts a full document which does not exist yet with default options.
   *
   * @param id the document id to insert.
   * @param content the document content to insert.
   * @return a {@link Mono} completing once inserted or failed.
   */
  public Mono<MutationResult> insert(final String id, Object content) {
    return insert(id, content, DEFAULT_INSERT_OPTIONS);
  }

  /**
   * Inserts a full document which does not exist yet with custom options.
   *
   * @param id the document id to insert.
   * @param content the document content to insert.
   * @param options custom options to customize the insert behavior.
   * @return a {@link Mono} completing once inserted or failed.
   */
  public Mono<MutationResult> insert(final String id, Object content, final InsertOptions options) {
    notNull(options, "InsertOptions", () -> ReducedKeyValueErrorContext.create(id, asyncCollection.collectionIdentifier()));
    notNull(content, "Content", () -> ReducedKeyValueErrorContext.create(id, asyncCollection.collectionIdentifier()));

    InsertOptions.Built opts = options.build();
    Transcoder transcoder = opts.transcoder() == null ? environment().transcoder() : opts.transcoder();
    return kvOps.insertReactive(
        opts,
        id,
        () -> transcoder.encode(content),
        opts.toCoreDurability(),
        opts.expiry().encode()
      )
      .map(MutationResult::new);
  }

  /**
   * Upserts a full document which might or might not exist yet with default options.
   *
   * @param id the document id to upsert.
   * @param content the document content to upsert.
   * @return a {@link Mono} completing once upserted or failed.
   */
  public Mono<MutationResult> upsert(final String id, Object content) {
    return upsert(id, content, DEFAULT_UPSERT_OPTIONS);
  }

  /**
   * Upserts a full document which might or might not exist yet with custom options.
   *
   * @param id the document id to upsert.
   * @param content the document content to upsert.
   * @param options custom options to customize the upsert behavior.
   * @return a {@link Mono} completing once upserted or failed.
   */
  public Mono<MutationResult> upsert(final String id, Object content, final UpsertOptions options) {
    notNull(options, "UpsertOptions", () -> ReducedKeyValueErrorContext.create(id, asyncCollection.collectionIdentifier()));
    notNull(content, "Content", () -> ReducedKeyValueErrorContext.create(id, asyncCollection.collectionIdentifier()));

    UpsertOptions.Built opts = options.build();
    Transcoder transcoder = opts.transcoder() == null ? environment().transcoder() : opts.transcoder();
    return kvOps.upsertReactive(
        opts,
        id,
        () -> transcoder.encode(content),
        opts.toCoreDurability(),
        opts.expiry().encode(),
        opts.preserveExpiry()
      )
      .map(MutationResult::new);
  }

  /**
   * Replaces a full document which already exists with default options.
   *
   * @param id the document id to replace.
   * @param content the document content to replace.
   * @return a {@link Mono} completing once replaced or failed.
   */
  public Mono<MutationResult> replace(final String id, Object content) {
    return replace(id, content, DEFAULT_REPLACE_OPTIONS);
  }

  /**
   * Replaces a full document which already exists with custom options.
   *
   * @param id the document id to replace.
   * @param content the document content to replace.
   * @param options custom options to customize the replace behavior.
   * @return a {@link Mono} completing once replaced or failed.
   */
  public Mono<MutationResult> replace(final String id, Object content, final ReplaceOptions options) {
    notNull(options, "ReplaceOptions", () -> ReducedKeyValueErrorContext.create(id, asyncCollection.collectionIdentifier()));
    notNull(content, "Content", () -> ReducedKeyValueErrorContext.create(id, asyncCollection.collectionIdentifier()));

    ReplaceOptions.Built opts = options.build();
    Transcoder transcoder = opts.transcoder() == null ? environment().transcoder() : opts.transcoder();
    return kvOps.replaceReactive(
        opts,
        id,
        () -> transcoder.encode(content),
        opts.cas(),
        opts.toCoreDurability(),
        opts.expiry().encode(),
        opts.preserveExpiry()
      )
      .map(MutationResult::new);
  }

  /**
   * Updates the expiry of the document with the given id with default options.
   *
   * @param id the id of the document to update.
   * @param expiry the new expiry for the document.
   * @return a {@link MutationResult} once the operation completes.
   */
  public Mono<MutationResult> touch(final String id, final Duration expiry) {
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
  public Mono<MutationResult> touch(final String id, final Duration expiry, final TouchOptions options) {
    notNull(options, "TouchOptions", () -> ReducedKeyValueErrorContext.create(id, asyncCollection.collectionIdentifier()));
    notNull(expiry, "Expiry", () -> ReducedKeyValueErrorContext.create(id, asyncCollection.collectionIdentifier()));

    TouchOptions.Built opts = options.build();
    return kvOps.touchReactive(opts, id, Expiry.relative(expiry).encode())
      .map(MutationResult::new);
  }

  /**
   * Unlocks a document if it has been locked previously, with default options.
   *
   * @param id the id of the document.
   * @param cas the CAS value which is needed to unlock it.
   * @return the mono which completes once a response has been received.
   */
  public Mono<Void> unlock(final String id, final long cas) {
    return unlock(id, cas, DEFAULT_UNLOCK_OPTIONS);
  }

  /**
   * Unlocks a document if it has been locked previously, with custom options.
   *
   * @param id the id of the document.
   * @param cas the CAS value which is needed to unlock it.
   * @param options the options to customize.
   * @return the mono which completes once a response has been received.
   */
  public Mono<Void> unlock(final String id, final long cas, final UnlockOptions options) {
    notNull(options, "UnlockOptions", () -> ReducedKeyValueErrorContext.create(id, asyncCollection.collectionIdentifier()));
    UnlockOptions.Built opts = options.build();
    return kvOps.unlockReactive(opts, id, cas);
  }

  /**
   * Performs lookups to document fragments with default options.
   *
   * @param id the outer document ID.
   * @param specs the spec which specifies the type of lookups to perform.
   * @return the {@link LookupInResult} once the lookup has been performed or failed.
   */
  public Mono<LookupInResult> lookupIn(final String id, List<LookupInSpec> specs) {
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
  public Mono<LookupInResult> lookupIn(final String id, List<LookupInSpec> specs, final LookupInOptions options) {
    notNull(options, "LookupInOptions", () -> ReducedKeyValueErrorContext.create(id, async().collectionIdentifier()));
    notNull(specs, "LookupInSpecs", () -> ReducedKeyValueErrorContext.create(id, async().collectionIdentifier()));

    LookupInOptions.Built opts = options.build();
    JsonSerializer serializer = opts.serializer() == null ? environment().jsonSerializer() : opts.serializer();

    return kvOps.subdocGetReactive(
        opts,
        id,
        transform(specs, LookupInSpec::toCore),
        opts.accessDeleted()
      )
      .map(it -> new LookupInResult(it, serializer));
  }

  /**
   * Performs mutations to document fragments with default options.
   *
   * @param id the outer document ID.
   * @param specs the spec which specifies the type of mutations to perform.
   * @return the {@link MutateInResult} once the mutation has been performed or failed.
   */
  public Mono<MutateInResult> mutateIn(final String id, final List<MutateInSpec> specs) {
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
  public Mono<MutateInResult> mutateIn(final String id, final List<MutateInSpec> specs,
                                       final MutateInOptions options) {
    notNull(options, "MutateInOptions", () -> ReducedKeyValueErrorContext.create(id, async().collectionIdentifier()));
    notNull(specs, "MutationSpecs", () -> ReducedKeyValueErrorContext.create(id, async().collectionIdentifier()));

    MutateInOptions.Built opts = options.build();
    JsonSerializer serializer = opts.serializer() == null ? environment().jsonSerializer() : opts.serializer();
    return kvOps.subdocMutateReactive(
        opts,
        id,
        () -> transform(specs, it -> it.toCore(serializer)),
        opts.storeSemantics().toCore(),
        opts.cas(),
        opts.toCoreDurability(),
        opts.expiry().encode(),
        opts.preserveExpiry(),
        opts.accessDeleted(),
        opts.createAsDeleted()
      )
      .map(it -> new MutateInResult(it, serializer));
  }

  /**
   * Returns a stream of {@link ScanResult ScanResults} performing a Key-Value range scan with default options.
   *
   * @param scanType the type or range scan to perform.
   * @return a Flux of {@link ScanResult ScanResults} (potentially empty).
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  @Stability.Volatile
  public Flux<ScanResult> scan(final ScanType scanType) {
    return scan(scanType, ScanOptions.scanOptions());
  }

  /**
   * Returns a stream of {@link ScanResult ScanResults} performing a Key-Value range scan with custom options.
   *
   * @param scanType the type or range scan to perform.
   * @param options a {@link ScanOptions} to customize the behavior of the scan operation.
   * @return a Flux of {@link ScanResult ScanResults} (potentially empty).
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  @Stability.Volatile
  public Flux<ScanResult> scan(final ScanType scanType, final ScanOptions options) {
    notNull(scanType, "ScanType",
        () -> ReducedKeyValueErrorContext.create(null, asyncCollection.collectionIdentifier()));
    ScanOptions.Built opts = notNull(options, "ScanOptions",
        () -> ReducedKeyValueErrorContext.create(null, asyncCollection.collectionIdentifier())).build();
    return kvOps.scanRequestReactive(scanType.build(), opts).map(
        r -> new ScanResult(opts.idsOnly(), r.key(), r.value(), r.flags(), r.cas(), Optional.ofNullable(r.expiry()),
            opts.transcoder() != null ? opts.transcoder() : environment().transcoder()));
  }

}
