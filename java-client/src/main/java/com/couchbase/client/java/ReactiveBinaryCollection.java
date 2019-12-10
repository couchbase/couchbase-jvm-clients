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
import com.couchbase.client.core.Reactor;
import com.couchbase.client.core.error.CasMismatchException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.error.ReducedKeyValueErrorContext;
import com.couchbase.client.core.error.TimeoutException;
import com.couchbase.client.core.msg.kv.AppendRequest;
import com.couchbase.client.core.msg.kv.DecrementRequest;
import com.couchbase.client.core.msg.kv.IncrementRequest;
import com.couchbase.client.core.msg.kv.PrependRequest;
import com.couchbase.client.java.kv.AppendAccessor;
import com.couchbase.client.java.kv.AppendOptions;
import com.couchbase.client.java.kv.CounterAccessor;
import com.couchbase.client.java.kv.CounterResult;
import com.couchbase.client.java.kv.DecrementOptions;
import com.couchbase.client.java.kv.IncrementOptions;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.PrependAccessor;
import com.couchbase.client.java.kv.PrependOptions;
import reactor.core.publisher.Mono;

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.java.AsyncBinaryCollection.DEFAULT_APPEND_OPTIONS;
import static com.couchbase.client.java.AsyncBinaryCollection.DEFAULT_DECREMENT_OPTIONS;
import static com.couchbase.client.java.AsyncBinaryCollection.DEFAULT_INCREMENT_OPTIONS;
import static com.couchbase.client.java.AsyncBinaryCollection.DEFAULT_PREPEND_OPTIONS;

/**
 * Allows to perform certain operations on non-JSON documents.
 */
public class ReactiveBinaryCollection {

  /**
   * Holds the underlying async binary collection.
   */
  private final AsyncBinaryCollection async;

  /**
   * Provides access to the core.
   */
  private final Core core;

  ReactiveBinaryCollection(final Core core, final AsyncBinaryCollection async) {
    this.core = core;
    this.async = async;
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
    return Mono.defer(() -> {
      notNull(options, "AppendOptions", () -> ReducedKeyValueErrorContext.create(id, async.collectionIdentifier()));
      AppendOptions.Built opts = options.build();
      AppendRequest request = async.appendRequest(id, content, opts);
      return Reactor.wrap(
        request,
        AppendAccessor.append(core, request, id, opts.persistTo(), opts.replicateTo()),
        true
      );
    });
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
    return Mono.defer(() -> {
      notNull(options, "PrependOptions", () -> ReducedKeyValueErrorContext.create(id, async.collectionIdentifier()));
      PrependOptions.Built opts = options.build();
      PrependRequest request = async.prependRequest(id, content, opts);
      return Reactor.wrap(
        request,
        PrependAccessor.prepend(core, request, id, opts.persistTo(), opts.replicateTo()),
        true
      );
    });
  }

  /**
   * Increments the counter document by one.
   *
   * @param id the document id which is used to uniquely identify it.
   * @return a {@link CounterResult} once completed.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws CasMismatchException if the document has been concurrently modified on the server.
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
   * @throws CasMismatchException if the document has been concurrently modified on the server.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public Mono<CounterResult> increment(final String id, final IncrementOptions options) {
    return Mono.defer(() -> {
      notNull(options, "IncrementOptions", () -> ReducedKeyValueErrorContext.create(id, async.collectionIdentifier()));
      IncrementOptions.Built opts = options.build();
      IncrementRequest request = async.incrementRequest(id, opts);
      return Reactor.wrap(
        request,
        CounterAccessor.increment(core, request, id, opts.persistTo(), opts.replicateTo()),
        true
      );
    });
  }

  /**
   * Decrements the counter document by one.
   *
   * @param id the document id which is used to uniquely identify it.
   * @return a {@link CounterResult} once completed.
   * @throws DocumentNotFoundException the given document id is not found in the collection.
   * @throws CasMismatchException if the document has been concurrently modified on the server.
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
   * @throws CasMismatchException if the document has been concurrently modified on the server.
   * @throws TimeoutException if the operation times out before getting a result.
   * @throws CouchbaseException for all other error reasons (acts as a base type and catch-all).
   */
  public Mono<CounterResult> decrement(final String id, final DecrementOptions options) {
    return Mono.defer(() -> {
      notNull(options, "DecrementOptions", () -> ReducedKeyValueErrorContext.create(id, async.collectionIdentifier()));
      DecrementOptions.Built opts = options.build();
      DecrementRequest request = async.decrementRequest(id, opts);
      return Reactor.wrap(
        request,
        CounterAccessor.decrement(core, request, id, opts.persistTo(), opts.replicateTo()),
        true
      );
    });
  }

}
