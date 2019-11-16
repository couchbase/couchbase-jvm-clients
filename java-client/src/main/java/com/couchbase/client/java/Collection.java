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
import com.couchbase.client.core.error.CasMismatchException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.DocumentUnretrievableException;
import com.couchbase.client.core.error.RequestTimeoutException;
import com.couchbase.client.core.error.subdoc.SubDocumentException;
import com.couchbase.client.java.datastructures.CouchbaseArrayList;
import com.couchbase.client.java.datastructures.CouchbaseArraySet;
import com.couchbase.client.java.datastructures.CouchbaseMap;
import com.couchbase.client.java.datastructures.CouchbaseQueue;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.kv.ArrayListOptions;
import com.couchbase.client.java.kv.ArraySetOptions;
import com.couchbase.client.java.kv.ExistsOptions;
import com.couchbase.client.java.kv.ExistsResult;
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
import com.couchbase.client.java.kv.TouchOptions;
import com.couchbase.client.java.kv.UnlockOptions;
import com.couchbase.client.java.kv.UpsertOptions;

import java.time.Duration;
import java.util.List;
import java.util.stream.Stream;

import static com.couchbase.client.java.AsyncUtils.block;
import static com.couchbase.client.java.ReactiveCollection.DEFAULT_GET_ALL_REPLICAS_OPTIONS;

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
   * Creates a new {@link Collection}.
   *
   * @param asyncCollection the underlying async collection.
   */
  Collection(final AsyncCollection asyncCollection) {
    this.asyncCollection = asyncCollection;
    reactiveCollection = new ReactiveCollection(asyncCollection);
    binaryCollection = new BinaryCollection(asyncCollection.binary());
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
   * @throws RequestTimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public GetResult get(final String id) {
    return block(async().get(id));
  }

  /**
   * Fetches the full document from this collection with custom options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param options options to customize the get request.
   * @return a {@link GetResult} once the document has been loaded.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws RequestTimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public GetResult get(final String id, final GetOptions options) {
    return block(async().get(id, options));
  }

  /**
   * Fetches a full document and write-locks it for the given duration.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param lockTime how long to lock the document for (values over 30 seconds will be capped).
   * @return a {@link GetResult} once the document has been locked and loaded.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws RequestTimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public GetResult getAndLock(final String id, final Duration lockTime) {
    return block(async().getAndLock(id, lockTime));
  }

  /**
   * Fetches a full document and write-locks it for the given duration with custom options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param lockTime how long to lock the document for (values over 30 seconds will be capped).
   * @param options options to customize the get and lock request.
   * @return a {@link GetResult} once the document has been loaded.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws RequestTimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public GetResult getAndLock(final String id, final Duration lockTime, final GetAndLockOptions options) {
    return block(async().getAndLock(id, lockTime, options));
  }

  /**
   * Fetches a full document and resets its expiration time to the expiry provided.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param expiry the new expiration time for the document.
   * @return a {@link GetResult} once the document has been loaded.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws RequestTimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public GetResult getAndTouch(final String id, final Duration expiry) {
    return block(async().getAndTouch(id, expiry));
  }

  /**
   * Fetches a full document and resets its expiration time to the expiry provided with custom options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param expiry the new expiration time for the document.
   * @param options options to customize the get and touch request.
   * @return a {@link GetResult} once the document has been loaded.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws RequestTimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public GetResult getAndTouch(final String id, final Duration expiry, final GetAndTouchOptions options) {
    return block(async().getAndTouch(id, expiry, options));
  }

  /**
   * Reads from all available replicas and the active node and returns the results as a stream.
   * <p>
   * Note that individual errors are ignored, so you can think of this API as a best effort
   * approach which explicitly emphasises availability over consistency.
   *
   * @param id the document id which is used to uniquely identify it.
   * @return a stream of results from the active and the replica.
   * @throws RequestTimeoutException if the operation times out before getting a result.
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
   * @throws RequestTimeoutException if the operation times out before getting a result.
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
   * @throws RequestTimeoutException if the operation times out before getting a result.
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
   * @throws RequestTimeoutException if the operation times out before getting a result.
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
   * @throws RequestTimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public ExistsResult exists(final String id) {
    return block(async().exists(id));
  }

  /**
   * Checks if the given document ID exists on the active partition with custom options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @return a {@link ExistsResult} completing once loaded or failed.
   * @throws RequestTimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public ExistsResult exists(final String id, final ExistsOptions options) {
    return block(async().exists(id, options));
  }

  /**
   * Removes a Document from a collection.
   *
   * @param id the document id which is used to uniquely identify it.
   * @return a {@link MutationResult} once removed.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws CasMismatchException if the document has been concurrently modified on the server.
   * @throws RequestTimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public MutationResult remove(final String id) {
    return block(async().remove(id));
  }

  /**
   * Removes a Document from a collection with custom options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param options custom options to change the default behavior.
   * @return a {@link MutationResult} once removed.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws CasMismatchException if the document has been concurrently modified on the server.
   * @throws RequestTimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public MutationResult remove(final String id, final RemoveOptions options) {
    return block(async().remove(id, options));
  }

  /**
   * Inserts a full document which does not exist yet.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param content the document content to insert.
   * @return a {@link MutationResult} once inserted.
   * @throws DocumentExistsException the given document id is already present in the collection.
   * @throws RequestTimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public MutationResult insert(final String id, final Object content) {
    return block(async().insert(id, content));
  }

  /**
   * Inserts a full document which does not exist yet with custom options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param content the document content to insert.
   * @param options custom options to customize the insert behavior.
   * @return a {@link MutationResult} once inserted.
   * @throws DocumentExistsException the given document id is already present in the collection.
   * @throws RequestTimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public MutationResult insert(final String id, final Object content, final InsertOptions options) {
    return block(async().insert(id, content, options));
  }

  /**
   * Upserts a full document which might or might not exist yet.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param content the document content to upsert.
   * @return a {@link MutationResult} once upserted.
   * @throws RequestTimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public MutationResult upsert(final String id, final Object content) {
    return block(async().upsert(id, content));
  }

  /**
   * Upserts a full document which might or might not exist yet with custom options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param content the document content to upsert.
   * @param options custom options to customize the upsert behavior.
   * @return a {@link MutationResult} once upserted.
   * @throws RequestTimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public MutationResult upsert(final String id, final Object content, final UpsertOptions options) {
    return block(async().upsert(id, content, options));
  }

  /**
   * Replaces a full document which already exists.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param content the document content to replace.
   * @return a {@link MutationResult} once replaced.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws CasMismatchException if the document has been concurrently modified on the server.
   * @throws RequestTimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public MutationResult replace(final String id, final Object content) {
    return block(async().replace(id, content));
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
   * @throws RequestTimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public MutationResult replace(final String id, final Object content, final ReplaceOptions options) {
    return block(async().replace(id, content, options));
  }

  /**
   * Updates the expiry of the document with the given id.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param expiry the new expiry for the document.
   * @return a {@link MutationResult} once the operation completes.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws RequestTimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public MutationResult touch(final String id, Duration expiry) {
    return block(async().touch(id, expiry));
  }

  /**
   * Updates the expiry of the document with the given id with custom options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param expiry the new expiry for the document.
   * @param options the custom options.
   * @return a {@link MutationResult} once the operation completes.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws RequestTimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public MutationResult touch(final String id, Duration expiry, final TouchOptions options) {
    return block(async().touch(id, expiry, options));
  }

  /**
   * Unlocks a document if it has been locked previously.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param cas the CAS value which is needed to unlock it.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws CasMismatchException if the document has been concurrently modified on the server.
   * @throws RequestTimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public void unlock(final String id, long cas) {
    block(async().unlock(id, cas));
  }


  /**
   * Unlocks a document if it has been locked previously, with custom options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param cas the CAS value which is needed to unlock it.
   * @param options the options to customize.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws CasMismatchException if the document has been concurrently modified on the server.
   * @throws RequestTimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public void unlock(final String id, long cas, final UnlockOptions options) {
    block(async().unlock(id, cas, options));
  }

  /**
   * Performs lookups to document fragments with default options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param specs the spec which specifies the type of lookups to perform.
   * @return the {@link LookupInResult} once the lookup has been performed or failed.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws RequestTimeoutException if the operation times out before getting a result.
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
   * @throws RequestTimeoutException if the operation times out before getting a result.
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
   * @throws RequestTimeoutException if the operation times out before getting a result.
   * @throws SubDocumentException if the server reports issues when applying the individual subdocument specs.
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
   * @throws RequestTimeoutException if the operation times out before getting a result.
   * @throws SubDocumentException if the server reports issues when applying the individual subdocument specs.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public MutateInResult mutateIn(final String id, final List<MutateInSpec> specs, final MutateInOptions options) {
    return block(async().mutateIn(id, specs, options));
  }

  /**
   * Returns a {@link CouchbaseArrayList<T>} backed by this collection, creating a
   * new empty one if none exists already
   *
   * @param id the list's document id.
   * @param entityType the class of the values contained in the set
   * @param options a {@link ArrayListOptions} to use for all operations on this instance of the list.
   * @return a {@link CouchbaseArrayList<T>}.
   */
  public <T> CouchbaseArrayList<T> list(final String id, Class<T> entityType, ArrayListOptions options) {
    return new CouchbaseArrayList<>(id, this, entityType, options);
  }

  /**
   * Returns a {@link CouchbaseArrayList<T>} backed by this collection, creating a
   * new empty one if none exists already
   *
   * @param id the list's document id.
   * @param entityType the class of the values contained in the set
   * @return a {@link CouchbaseArrayList<T>}.
\   */
  public <T> CouchbaseArrayList<T> list(final String id, Class<T> entityType) {
    return new CouchbaseArrayList<>(id, this, entityType);
  }

  /**
   * Returns a {@link CouchbaseArraySet<T>} backed by this collection, create a new
   * empty one if none exists already.
   *
   * @param id the set's document id.
   * @param entityType the class of the values contained in the set
   * @param options a {@link ArraySetOptions} to use for all operations on this instance of the set.
   * @return a {@link CouchbaseArraySet<T>}.
   */
  public <T> CouchbaseArraySet<T> set(final String id, final Class<T> entityType, ArraySetOptions options) {
    return new CouchbaseArraySet<>(id, this, entityType, options);
  }

  /**
   * Returns a {@link CouchbaseArraySet<T>} backed by this collection, create a new
   * empty one if none exists already.
   *
   * @param id the set's document id.
   * @param entityType the class of the values contained in the set
   * @return a {@link CouchbaseArraySet<T>}.
   */
  public <T> CouchbaseArraySet<T> set(final String id, final Class<T> entityType) {
    return new CouchbaseArraySet<>(id, this, entityType);
  }

  /**
   * Returns a {@link CouchbaseMap<T>} backed by this collection, creating a new
   * empty one if none exists already.  This map will have {@link String} keys, and
   * values of Class<T>
   *
   * @param id the map's document id.
   * @param entityType the class of the values contained the map, the keys are {@link String}s.
   * @param options a {@link MapOptions} to use for all operations on this instance of the map.
   * @return a {@link CouchbaseMap<T>}.
   */
  public <T> CouchbaseMap<T> map(final String id, final Class<T> entityType, MapOptions options) {
    return new CouchbaseMap<>(id, this, entityType, options);
  }

  /**
   * Returns a {@link CouchbaseMap<T>} backed by this collection, creating a new
   * empty one if none exists already.  This map will have {@link String} keys, and
   * values of Class<T>
   *
   * @param id the map's document id.
   * @param entityType the class of the values contained the map, the keys are {@link String}s.
   * @return a {@link CouchbaseMap<T>}.
   */
  public <T> CouchbaseMap<T> map(final String id, final Class<T> entityType) {
    return new CouchbaseMap<>(id, this, entityType);
  }

  /**
   * Returns a {@link CouchbaseQueue<T>} backed by this collection, creating a new
   * empty one if none exists.
   *
   * @param id the queue's document id.
   * @param entityType the class of the values contained in the queue.
   * @param options a {@link QueueOptions} to use for all operations on this instance of the queue.
   * @return a {@link CouchbaseQueue<T>}.
   */
  public <T> CouchbaseQueue<T> queue(final String id, final Class<T> entityType, QueueOptions options) {
    return new CouchbaseQueue<>(id, this, entityType, options);
  }

  /**
   * Returns a {@link CouchbaseQueue<T>} backed by this collection, creating a new
   * empty one if none exists.
   *
   * @param id the queue's document id.
   * @param entityType the class of the values contained in the queue.
   * @return a {@link CouchbaseQueue<T>}.
   */
  public <T> CouchbaseQueue<T> queue(final String id, final Class<T> entityType) {
    return new CouchbaseQueue<>(id, this, entityType);
  }
}
