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

import com.couchbase.client.core.CoreKeyspace;
import com.couchbase.client.core.api.CoreCouchbaseOps;
import com.couchbase.client.core.api.kv.CoreKvBinaryOps;
import com.couchbase.client.core.error.CasMismatchException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.core.error.context.ReducedKeyValueErrorContext;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.java.kv.AppendOptions;
import com.couchbase.client.java.kv.CounterResult;
import com.couchbase.client.java.kv.DecrementOptions;
import com.couchbase.client.java.kv.IncrementOptions;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.PrependOptions;

import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.java.BinaryCollection.DEFAULT_APPEND_OPTIONS;
import static com.couchbase.client.java.BinaryCollection.DEFAULT_DECREMENT_OPTIONS;
import static com.couchbase.client.java.BinaryCollection.DEFAULT_INCREMENT_OPTIONS;
import static com.couchbase.client.java.BinaryCollection.DEFAULT_PREPEND_OPTIONS;
import static java.util.Objects.requireNonNull;

/**
 * Allows to perform certain operations on non-JSON documents.
 */
public class AsyncBinaryCollection {

  final CoreKvBinaryOps coreKvBinaryOps;
  private final CoreKeyspace keyspace;

  AsyncBinaryCollection(
    final CoreKeyspace keyspace,
    final CoreCouchbaseOps couchbaseOps
  ) {
    this.keyspace = requireNonNull(keyspace);
    this.coreKvBinaryOps = couchbaseOps.kvBinaryOps(keyspace);
  }

  /**
   * Appends binary content to the document.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param content the binary content to append to the document.
   * @return a {@link MutationResult} once completed.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws CasMismatchException if the document has been concurrently modified on the server.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public CompletableFuture<MutationResult> append(final String id, final byte[] content) {
    return append(id, content, DEFAULT_APPEND_OPTIONS);
  }

  /**
   * Appends binary content to the document with custom options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param content the binary content to append to the document.
   * @param options custom options to customize the append behavior.
   * @return a {@link MutationResult} once completed.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws CasMismatchException if the document has been concurrently modified on the server.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public CompletableFuture<MutationResult> append(final String id, final byte[] content, final AppendOptions options) {
    notNull(options, "AppendOptions", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier()));
    AppendOptions.Built opts = notNull(options, "options").build();
    return coreKvBinaryOps.appendAsync(id, content, opts, opts.cas(), opts.toCoreDurability())
        .thenApply(MutationResult::new);
  }

  /**
   * Prepends binary content to the document.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param content the binary content to append to the document.
   * @return a {@link MutationResult} once completed.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws CasMismatchException if the document has been concurrently modified on the server.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public CompletableFuture<MutationResult> prepend(final String id, final byte[] content) {
    return prepend(id, content, DEFAULT_PREPEND_OPTIONS);
  }

  /**
   * Prepends binary content to the document with custom options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param content the binary content to append to the document.
   * @param options custom options to customize the prepend behavior.
   * @return a {@link MutationResult} once completed.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws CasMismatchException if the document has been concurrently modified on the server.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public CompletableFuture<MutationResult> prepend(final String id, final byte[] content,
      final PrependOptions options) {
    notNull(options, "PrependOptions", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier()));
    PrependOptions.Built opts = options.build();
    return coreKvBinaryOps.prependAsync(id, content, opts, opts.cas(), opts.toCoreDurability())
        .thenApply(MutationResult::new);
  }

  /**
   * Increments the counter document by one.
   *
   * @param id the document id which is used to uniquely identify it.
   * @return a {@link CounterResult} once completed.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public CompletableFuture<CounterResult> increment(final String id) {
    return increment(id, DEFAULT_INCREMENT_OPTIONS);
  }

  /**
   * Increments the counter document by one or the number defined in the options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param options custom options to customize the increment behavior.
   * @return a {@link CounterResult} once completed.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public CompletableFuture<CounterResult> increment(final String id, final IncrementOptions options) {
    notNull(options, "IncrementOptions", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier()));
    IncrementOptions.Built opts = options.build();
    return coreKvBinaryOps
        .incrementAsync(id, opts, opts.expiry().encode(), opts.delta(), opts.initial(), opts.toCoreDurability())
        .thenApply(CounterResult::new);
  }

  /**
   * Decrements the counter document by one.
   *
   * @param id the document id which is used to uniquely identify it.
   * @return a {@link CounterResult} once completed.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public CompletableFuture<CounterResult> decrement(final String id) {
    return decrement(id, DEFAULT_DECREMENT_OPTIONS);
  }

  /**
   * Decrements the counter document by one or the number defined in the options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param options custom options to customize the decrement behavior.
   * @return a {@link CounterResult} once completed.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public CompletableFuture<CounterResult> decrement(final String id, final DecrementOptions options) {
    notNull(options, "DecrementOptions", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier()));
    DecrementOptions.Built opts = options.build();
    return coreKvBinaryOps
        .decrementAsync(id, opts, opts.expiry().encode(), opts.delta(), opts.initial(), opts.toCoreDurability())
        .thenApply(CounterResult::new);
  }

  CollectionIdentifier collectionIdentifier() {
    return keyspace.toCollectionIdentifier();
  }
}
