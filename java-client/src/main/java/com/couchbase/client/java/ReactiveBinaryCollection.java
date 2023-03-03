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

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.java.BinaryCollection.DEFAULT_APPEND_OPTIONS;
import static com.couchbase.client.java.BinaryCollection.DEFAULT_DECREMENT_OPTIONS;
import static com.couchbase.client.java.BinaryCollection.DEFAULT_INCREMENT_OPTIONS;
import static com.couchbase.client.java.BinaryCollection.DEFAULT_PREPEND_OPTIONS;

import com.couchbase.client.core.io.CollectionIdentifier;
import reactor.core.publisher.Mono;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.api.kv.CoreKvBinaryOps;
import com.couchbase.client.core.error.CasMismatchException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.core.error.context.ReducedKeyValueErrorContext;
import com.couchbase.client.java.kv.AppendOptions;
import com.couchbase.client.java.kv.CounterResult;
import com.couchbase.client.java.kv.DecrementOptions;
import com.couchbase.client.java.kv.IncrementOptions;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.PrependOptions;

/**
 * Allows to perform certain operations on non-JSON documents.
 */
public class ReactiveBinaryCollection {


  private final CollectionIdentifier collectionIdentifier;
  private final CoreKvBinaryOps coreKvBinaryOps;

  ReactiveBinaryCollection(final AsyncBinaryCollection async) {
    this.collectionIdentifier = async.collectionIdentifier();
    this.coreKvBinaryOps = async.coreKvBinaryOps;
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
  public Mono<MutationResult> append(final String id, final byte[] content) {
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
  public Mono<MutationResult> append(final String id, final byte[] content, final AppendOptions options) {
    AppendOptions.Built opts = notNull(options, "AppendOptions", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier)).build();
    return coreKvBinaryOps.appendReactive(id, content, opts, opts.cas(), opts.toCoreDurability()).map(MutationResult::new);
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
  public Mono<MutationResult> prepend(final String id, final byte[] content) {
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
  public Mono<MutationResult> prepend(final String id, final byte[] content, final PrependOptions options) {
      PrependOptions.Built opts = notNull(options, "PrependOptions", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier)).build();
    return coreKvBinaryOps.prependReactive(id, content, opts, opts.cas(), opts.toCoreDurability()).map(MutationResult::new);
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
  public Mono<CounterResult> increment(final String id) {
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
  public Mono<CounterResult> increment(final String id, final IncrementOptions options) {
      IncrementOptions.Built opts = notNull(options, "IncrementOptions", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier)).build();
      return coreKvBinaryOps.incrementReactive(id, opts, opts.expiry().encode(), opts.delta(), opts.initial(),
          opts.toCoreDurability()).map(CounterResult::new);
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
  public Mono<CounterResult> decrement(final String id) {
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
  public Mono<CounterResult> decrement(final String id, final DecrementOptions options) {
      DecrementOptions.Built opts = notNull(options, "DecrementOptions", () -> ReducedKeyValueErrorContext.create(id, collectionIdentifier)).build();
      return coreKvBinaryOps.decrementReactive(id, opts, opts.expiry().encode(), opts.delta(), opts.initial(),
        opts.toCoreDurability()).map(CounterResult::new);
  }

}
