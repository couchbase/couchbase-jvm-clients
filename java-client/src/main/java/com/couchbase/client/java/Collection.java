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
import com.couchbase.client.core.error.CasMismatchException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.DocumentUnretrievableException;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.core.error.context.ReducedKeyValueErrorContext;
import com.couchbase.client.java.codec.Transcoder;
import com.couchbase.client.java.datastructures.CouchbaseArrayList;
import com.couchbase.client.java.datastructures.CouchbaseArraySet;
import com.couchbase.client.java.datastructures.CouchbaseMap;
import com.couchbase.client.java.datastructures.CouchbaseQueue;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.kv.ArrayListOptions;
import com.couchbase.client.java.kv.ArraySetOptions;
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
import com.couchbase.client.java.kv.MapOptions;
import com.couchbase.client.java.kv.MutateInOptions;
import com.couchbase.client.java.kv.MutateInResult;
import com.couchbase.client.java.kv.MutateInSpec;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.QueueOptions;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.ReplaceOptions;
import com.couchbase.client.java.kv.ScanOptions;
import com.couchbase.client.java.kv.ScanResult;
import com.couchbase.client.java.kv.ScanType;
import com.couchbase.client.java.kv.TouchOptions;
import com.couchbase.client.java.kv.UnlockOptions;
import com.couchbase.client.java.kv.UpsertOptions;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Stream;

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.java.AsyncUtils.block;
import static com.couchbase.client.java.ReactiveCollection.DEFAULT_EXISTS_OPTIONS;
import static com.couchbase.client.java.ReactiveCollection.DEFAULT_GET_ALL_REPLICAS_OPTIONS;
import static com.couchbase.client.java.ReactiveCollection.DEFAULT_GET_AND_LOCK_OPTIONS;
import static com.couchbase.client.java.ReactiveCollection.DEFAULT_GET_AND_TOUCH_OPTIONS;
import static com.couchbase.client.java.ReactiveCollection.DEFAULT_GET_OPTIONS;
import static com.couchbase.client.java.ReactiveCollection.DEFAULT_INSERT_OPTIONS;
import static com.couchbase.client.java.ReactiveCollection.DEFAULT_REMOVE_OPTIONS;
import static com.couchbase.client.java.ReactiveCollection.DEFAULT_REPLACE_OPTIONS;
import static com.couchbase.client.java.ReactiveCollection.DEFAULT_TOUCH_OPTIONS;
import static com.couchbase.client.java.ReactiveCollection.DEFAULT_UNLOCK_OPTIONS;
import static com.couchbase.client.java.ReactiveCollection.DEFAULT_UPSERT_OPTIONS;
import static com.couchbase.client.java.kv.ArrayListOptions.arrayListOptions;
import static com.couchbase.client.java.kv.ArraySetOptions.arraySetOptions;
import static com.couchbase.client.java.kv.MapOptions.mapOptions;
import static com.couchbase.client.java.kv.QueueOptions.queueOptions;

/**
 * The {@link Collection} provides blocking, synchronous access to all collection APIs.
 *
 * <p>If asynchronous access is needed, we recommend looking at the {@link ReactiveCollection} and
 * if the last drop of performance is needed the {@link AsyncCollection}. This blocking API itself
 * is just a small layer on top of the {@link AsyncCollection} which blocks the current thread
 * until the request completes with a response.</p>
 *
 * @since 3.0.0
 */
public class Collection {

  /**
   * Holds the underlying async collection.
   */
  private final AsyncCollection asyncCollection;

  /**
   * Holds the related reactive collection.
   */
  private final ReactiveCollection reactiveCollection;

  /**
   * Holds the associated binary collection.
   */
  private final BinaryCollection binaryCollection;

  /**
   * Strategy for performing KV operations.
   */
  private final CoreKvOps kvOps;

  /**
   * Creates a new {@link Collection}.
   *
   * @param asyncCollection the underlying async collection.
   */
  Collection(final AsyncCollection asyncCollection) {
    this.asyncCollection = asyncCollection;
    reactiveCollection = new ReactiveCollection(asyncCollection);
    binaryCollection = new BinaryCollection(asyncCollection.binary());
    this.kvOps = asyncCollection.kvOps;
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
   * Provides access to the underlying {@link ReactiveCollection}.
   *
   * @return returns the underlying {@link ReactiveCollection}.
   */
  public ReactiveCollection reactive() {
    return reactiveCollection;
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
   * @return the {@link BinaryCollection}.
   */
  public BinaryCollection binary() {
    return binaryCollection;
  }

  /**
   * Fetches the full document from this collection.
   *
   * @param id the document id which is used to uniquely identify it.
   * @return a {@link GetResult} once the document has been loaded.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public GetResult get(final String id) {
    return get(id, DEFAULT_GET_OPTIONS);
  }

  /**
   * Fetches the full document from this collection with custom options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param options options to customize the get request.
   * @return a {@link GetResult} once the document has been loaded.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public GetResult get(final String id, final GetOptions options) {
    GetOptions.Built opts = notNull(options, "options").build();

    return new GetResult(
      kvOps.getBlocking(opts, id, opts.projections(), opts.withExpiry()),
      opts.transcoder() == null ? environment().transcoder() : opts.transcoder()
    );
  }

  /**
   * Fetches a full document and write-locks it for the given duration.
   * <p>
   * Note that the client does not enforce an upper limit on the {@link Duration} lockTime. The maximum lock time
   * by default on the server is 30 seconds. Any value larger than 30 seconds will be capped down by the server to
   * the default lock time, which is 15 seconds unless modified on the server side.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param lockTime how long to write-lock the document for (any duration > 30s will be capped to server default of 15s).
   * @return a {@link GetResult} once the document has been locked and loaded.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public GetResult getAndLock(final String id, final Duration lockTime) {
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
   * @param options options to customize the get and lock request.
   * @return a {@link GetResult} once the document has been loaded.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public GetResult getAndLock(final String id, final Duration lockTime, final GetAndLockOptions options) {
    GetAndLockOptions.Built opts = notNull(options, "options").build();

    return new GetResult(
      kvOps.getAndLockBlocking(opts, id, lockTime),
      opts.transcoder() == null ? environment().transcoder() : opts.transcoder()
    );
  }

  /**
   * Fetches a full document and resets its expiration time to the expiry provided.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param expiry the new expiration time for the document.
   * @return a {@link GetResult} once the document has been loaded.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public GetResult getAndTouch(final String id, final Duration expiry) {
    return getAndTouch(id, expiry, DEFAULT_GET_AND_TOUCH_OPTIONS);
  }

  /**
   * Fetches a full document and resets its expiration time to the expiry provided with custom options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param expiry the new expiration time for the document.
   * @param options options to customize the get and touch request.
   * @return a {@link GetResult} once the document has been loaded.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public GetResult getAndTouch(final String id, final Duration expiry, final GetAndTouchOptions options) {
    notNull(expiry, "expiry");
    GetAndTouchOptions.Built opts = notNull(options, "options").build();

    return new GetResult(
      kvOps.getAndTouchBlocking(opts, id, Expiry.relative(expiry).encode()),
      opts.transcoder() == null ? environment().transcoder() : opts.transcoder()
    );
  }

  /**
   * Reads from all available replicas and the active node and returns the results as a stream.
   * <p>
   * Note that individual errors are ignored, so you can think of this API as a best effort
   * approach which explicitly emphasises availability over consistency.
   *
   * @param id the document id which is used to uniquely identify it.
   * @return a stream of results from the active and the replica.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public Stream<GetReplicaResult> getAllReplicas(final String id) {
    return getAllReplicas(id, DEFAULT_GET_ALL_REPLICAS_OPTIONS);
  }

  /**
   * Reads all available or one replica and returns the results as a stream with custom options.
   * <p>
   * By default all available replicas and the active node will be asked and returned as
   * an async stream. If configured differently in the options
   *
   * @param id the document id which is used to uniquely identify it.
   * @param options the custom options.
   * @return a stream of results from the active and the replica depending on the options.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public Stream<GetReplicaResult> getAllReplicas(final String id, final GetAllReplicasOptions options) {
    return reactiveCollection.getAllReplicas(id, options).toStream();
  }

  /**
   * Reads all available replicas, and returns the first found.
   *
   * @param id the document id which is used to uniquely identify it.
   * @return the first available result, might be the active or a replica.
   * @throws DocumentUnretrievableException no document retrievable with a successful status.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public GetReplicaResult getAnyReplica(final String id) {
    return block(asyncCollection.getAnyReplica(id));
  }

  /**
   * Reads all available replicas, and returns the first found with custom options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param options the custom options.
   * @return the first available result, might be the active or a replica.
   * @throws DocumentUnretrievableException no document retrievable with a successful status.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public GetReplicaResult getAnyReplica(final String id, final GetAnyReplicaOptions options) {
    return block(asyncCollection.getAnyReplica(id, options));
  }

  /**
   * Checks if the given document ID exists on the active partition.
   *
   * @param id the document id which is used to uniquely identify it.
   * @return a {@link ExistsResult} completing once loaded or failed.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public ExistsResult exists(final String id) {
    return exists(id, DEFAULT_EXISTS_OPTIONS);
  }

  /**
   * Checks if the given document ID exists on the active partition with custom options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @return a {@link ExistsResult} completing once loaded or failed.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public ExistsResult exists(final String id, final ExistsOptions options) {
    ExistsOptions.Built opts = notNull(options, "options").build();
    return ExistsResult.from(kvOps.existsBlocking(opts, id));
  }

  /**
   * Removes a Document from a collection.
   *
   * @param id the document id which is used to uniquely identify it.
   * @return a {@link MutationResult} once removed.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws CasMismatchException if the document has been concurrently modified on the server.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public MutationResult remove(final String id) {
    return remove(id, DEFAULT_REMOVE_OPTIONS);
  }

  /**
   * Removes a Document from a collection with custom options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param options custom options to change the default behavior.
   * @return a {@link MutationResult} once removed.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws CasMismatchException if the document has been concurrently modified on the server.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public MutationResult remove(final String id, final RemoveOptions options) {
    notNull(options, "RemoveOptions", () -> ReducedKeyValueErrorContext.create(id, asyncCollection.collectionIdentifier()));
    RemoveOptions.Built opts = options.build();
    return new MutationResult(kvOps.removeBlocking(
      opts,
      id,
      opts.cas(),
      opts.toCoreDurability()
    ));
  }

  /**
   * Inserts a full document which does not exist yet.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param content the document content to insert.
   * @return a {@link MutationResult} once inserted.
   * @throws DocumentExistsException the given document id is already present in the collection.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public MutationResult insert(final String id, final Object content) {
    return insert(id, content, DEFAULT_INSERT_OPTIONS);
  }

  /**
   * Inserts a full document which does not exist yet with custom options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param content the document content to insert.
   * @param options custom options to customize the insert behavior.
   * @return a {@link MutationResult} once inserted.
   * @throws DocumentExistsException the given document id is already present in the collection.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public MutationResult insert(final String id, final Object content, final InsertOptions options) {
    notNull(options, "InsertOptions", () -> ReducedKeyValueErrorContext.create(id, asyncCollection.collectionIdentifier()));
    notNull(content, "Content", () -> ReducedKeyValueErrorContext.create(id, asyncCollection.collectionIdentifier()));

    InsertOptions.Built opts = options.build();
    Transcoder transcoder = opts.transcoder() == null ? environment().transcoder() : opts.transcoder();
    return new MutationResult(kvOps.insertBlocking(
      opts,
      id,
      () -> transcoder.encode(content),
      opts.toCoreDurability(),
      opts.expiry().encode()
    ));
  }

  /**
   * Upserts a full document which might or might not exist yet.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param content the document content to upsert.
   * @return a {@link MutationResult} once upserted.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public MutationResult upsert(final String id, final Object content) {
    return upsert(id, content, DEFAULT_UPSERT_OPTIONS);
  }

  /**
   * Upserts a full document which might or might not exist yet with custom options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param content the document content to upsert.
   * @param options custom options to customize the upsert behavior.
   * @return a {@link MutationResult} once upserted.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public MutationResult upsert(final String id, final Object content, final UpsertOptions options) {
    notNull(options, "UpsertOptions", () -> ReducedKeyValueErrorContext.create(id, asyncCollection.collectionIdentifier()));
    notNull(content, "Content", () -> ReducedKeyValueErrorContext.create(id, asyncCollection.collectionIdentifier()));

    UpsertOptions.Built opts = options.build();
    Transcoder transcoder = opts.transcoder() == null ? environment().transcoder() : opts.transcoder();
    return new MutationResult(kvOps.upsertBlocking(
      opts,
      id,
      () -> transcoder.encode(content),
      opts.toCoreDurability(),
      opts.expiry().encode(),
      opts.preserveExpiry()
    ));
  }

  /**
   * Replaces a full document which already exists.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param content the document content to replace.
   * @return a {@link MutationResult} once replaced.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws CasMismatchException if the document has been concurrently modified on the server.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public MutationResult replace(final String id, final Object content) {
    return replace(id, content, DEFAULT_REPLACE_OPTIONS);
  }

  /**
   * Replaces a full document which already exists with custom options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param content the document content to replace.
   * @param options custom options to customize the replace behavior.
   * @return a {@link MutationResult} once replaced.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws CasMismatchException if the document has been concurrently modified on the server.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public MutationResult replace(final String id, final Object content, final ReplaceOptions options) {
    notNull(options, "ReplaceOptions", () -> ReducedKeyValueErrorContext.create(id, asyncCollection.collectionIdentifier()));
    notNull(content, "Content", () -> ReducedKeyValueErrorContext.create(id, asyncCollection.collectionIdentifier()));

    ReplaceOptions.Built opts = options.build();
    Transcoder transcoder = opts.transcoder() == null ? environment().transcoder() : opts.transcoder();
    return new MutationResult(kvOps.replaceBlocking(
      opts,
      id,
      () -> transcoder.encode(content),
      opts.cas(),
      opts.toCoreDurability(),
      opts.expiry().encode(),
      opts.preserveExpiry()
    ));
  }

  /**
   * Updates the expiry of the document with the given id.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param expiry the new expiry for the document.
   * @return a {@link MutationResult} once the operation completes.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public MutationResult touch(final String id, Duration expiry) {
    return touch(id, expiry, DEFAULT_TOUCH_OPTIONS);
  }

  /**
   * Updates the expiry of the document with the given id with custom options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param expiry the new expiry for the document.
   * @param options the custom options.
   * @return a {@link MutationResult} once the operation completes.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public MutationResult touch(final String id, Duration expiry, final TouchOptions options) {
    notNull(options, "TouchOptions", () -> ReducedKeyValueErrorContext.create(id, asyncCollection.collectionIdentifier()));
    notNull(expiry, "Expiry", () -> ReducedKeyValueErrorContext.create(id, asyncCollection.collectionIdentifier()));

    TouchOptions.Built opts = options.build();
    return new MutationResult(
      kvOps.touchBlocking(opts, id, Expiry.relative(expiry).encode())
    );
  }

  /**
   * Unlocks a document if it has been locked previously.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param cas the CAS value which is needed to unlock it.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws CasMismatchException if the document has been concurrently modified on the server.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public void unlock(final String id, long cas) {
    unlock(id, cas, DEFAULT_UNLOCK_OPTIONS);
  }


  /**
   * Unlocks a document if it has been locked previously, with custom options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param cas the CAS value which is needed to unlock it.
   * @param options the options to customize.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws CasMismatchException if the document has been concurrently modified on the server.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public void unlock(final String id, long cas, final UnlockOptions options) {
    notNull(options, "UnlockOptions", () -> ReducedKeyValueErrorContext.create(id, asyncCollection.collectionIdentifier()));
    UnlockOptions.Built opts = options.build();
    kvOps.unlockBlocking(opts, id, cas);
  }

  /**
   * Performs lookups to document fragments with default options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param specs the spec which specifies the type of lookups to perform.
   * @return the {@link LookupInResult} once the lookup has been performed or failed.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public LookupInResult lookupIn(final String id, List<LookupInSpec> specs) {
    return block(async().lookupIn(id, specs));
  }

  /**
   * Performs lookups to document fragments with custom options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param specs the spec which specifies the type of lookups to perform.
   * @param options custom options to modify the lookup options.
   * @return the {@link LookupInResult} once the lookup has been performed or failed.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public LookupInResult lookupIn(final String id, List<LookupInSpec> specs, final LookupInOptions options) {
    return block(async().lookupIn(id, specs, options));
  }

  /**
   * Performs mutations to document fragments with default options.
   *
   * @param id the outer document ID.
   * @param specs the spec which specifies the type of mutations to perform.
   * @return the {@link MutateInResult} once the mutation has been performed or failed.
   * @throws DocumentNotFoundException the given document id is not found in the collection and replace mode is selected.
   * @throws DocumentExistsException the given document id is already present in the collection and insert is was selected.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public MutateInResult mutateIn(final String id, final List<MutateInSpec> specs) {
    return block(async().mutateIn(id, specs));
  }

  /**
   * Performs mutations to document fragments with custom options.
   *
   * @param id the outer document ID.
   * @param specs the spec which specifies the type of mutations to perform.
   * @param options custom options to modify the mutation options.
   * @return the {@link MutateInResult} once the mutation has been performed or failed.
   * @throws DocumentNotFoundException the given document id is not found in the collection and replace mode is selected.
   * @throws DocumentExistsException the given document id is already present in the collection and insert is was selected.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public MutateInResult mutateIn(final String id, final List<MutateInSpec> specs, final MutateInOptions options) {
    return block(async().mutateIn(id, specs, options));
  }

  /**
   * Returns a {@link CouchbaseArrayList} backed by this collection, creating a
   * new empty one if none exists already
   *
   * @param id the list's document id.
   * @param entityType the class of the values contained in the set
   * @return a {@link CouchbaseArrayList}.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public <T> List<T> list(final String id, final Class<T> entityType) {
    return list(id, entityType, arrayListOptions());
  }

  /**
   * Returns a {@link CouchbaseArrayList} backed by this collection, creating a
   * new empty one if none exists already
   *
   * @param id the list's document id.
   * @param entityType the class of the values contained in the set
   * @param options a {@link ArrayListOptions} to use for all operations on this instance of the list.
   * @return a {@link CouchbaseArrayList}.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public <T> List<T> list(final String id, final Class<T> entityType, final ArrayListOptions options) {
    return new CouchbaseArrayList<>(id, this, entityType, options);
  }

  /**
   * Returns a {@link CouchbaseArraySet} backed by this collection, create a new
   * empty one if none exists already.
   *
   * @param id the set's document id.
   * @param entityType the class of the values contained in the set
   * @return a {@link CouchbaseArraySet}.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public <T> Set<T> set(final String id, final Class<T> entityType) {
    return set(id, entityType, arraySetOptions());
  }

  /**
   * Returns a {@link CouchbaseArraySet} backed by this collection, create a new
   * empty one if none exists already.
   *
   * @param id the set's document id.
   * @param entityType the class of the values contained in the set
   * @param options a {@link ArraySetOptions} to use for all operations on this instance of the set.
   * @return a {@link CouchbaseArraySet}.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public <T> Set<T> set(final String id, final Class<T> entityType, ArraySetOptions options) {
    return new CouchbaseArraySet<>(id, this, entityType, options);
  }

  /**
   * Returns a {@link CouchbaseMap} backed by this collection, creating a new
   * empty one if none exists already.  This map will have {@link String} keys, and
   * values of Class<T>
   *
   * @param id the map's document id.
   * @param entityType the class of the values contained the map, the keys are {@link String}s.
   * @return a {@link CouchbaseMap}.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public <T> Map<String, T> map(final String id, final Class<T> entityType) {
    return map(id, entityType, mapOptions());
  }

  /**
   * Returns a {@link CouchbaseMap} backed by this collection, creating a new
   * empty one if none exists already.  This map will have {@link String} keys, and
   * values of Class<T>
   *
   * @param id the map's document id.
   * @param entityType the class of the values contained the map, the keys are {@link String}s.
   * @param options a {@link MapOptions} to use for all operations on this instance of the map.
   * @return a {@link CouchbaseMap}.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public <T> Map<String, T> map(final String id, final Class<T> entityType, MapOptions options) {
    return new CouchbaseMap<>(id, this, entityType, options);
  }

  /**
   * Returns a {@link CouchbaseQueue} backed by this collection, creating a new
   * empty one if none exists.
   *
   * @param id the queue's document id.
   * @param entityType the class of the values contained in the queue.
   * @return a {@link CouchbaseQueue}.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public <T> Queue<T> queue(final String id, final Class<T> entityType) {
    return queue(id, entityType, queueOptions());
  }

  /**
   * Returns a {@link CouchbaseQueue} backed by this collection, creating a new
   * empty one if none exists.
   *
   * @param id the queue's document id.
   * @param entityType the class of the values contained in the queue.
   * @param options a {@link QueueOptions} to use for all operations on this instance of the queue.
   * @return a {@link CouchbaseQueue}.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public <T> Queue<T> queue(final String id, final Class<T> entityType, QueueOptions options) {
    return new CouchbaseQueue<>(id, this, entityType, options);
  }

  /**
   * Returns a stream of {@link ScanResult ScanResults} performing a Key-Value range scan with default options.
   *
   * @param scanType the type or range scan to perform.
   * @return a stream of {@link ScanResult ScanResults} (potentially empty).
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  @Stability.Volatile
  public Stream<ScanResult> scan(final ScanType scanType) {
    return scan(scanType, ScanOptions.scanOptions());
  }

  /**
   * Returns a stream of {@link ScanResult ScanResults} performing a Key-Value range scan with custom options.
   *
   * @param scanType the type or range scan to perform.
   * @param options a {@link ScanOptions} to customize the behavior of the scan operation.
   * @return a stream of {@link ScanResult ScanResults} (potentially empty).
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  @Stability.Volatile
  public Stream<ScanResult> scan(final ScanType scanType, final ScanOptions options) {
    return asyncCollection.scanRequest(scanType, options).toStream();
  }

}
