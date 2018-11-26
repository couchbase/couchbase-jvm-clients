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
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.options.FullInsertOptions;
import com.couchbase.client.java.options.GetOptions;
import com.couchbase.client.java.options.InsertOptions;
import com.couchbase.client.java.options.RemoveOptions;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

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
   * Holds the underlying reactive collection.
   */
  private final ReactiveCollection reactiveCollection;

  public Collection(final String name, final String scope, final Core core,
             final CouchbaseEnvironment environment) {
    asyncCollection = new AsyncCollection(name, scope, core, environment);
    reactiveCollection = new ReactiveCollection(asyncCollection);
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
   * Fetches a Document (or a fragment of it) from a collection with default options.
   *
   * <p>The {@link Optional} indicates if the document has been found or not. If the document
   * has not been found, an empty optional will be returned.</p>
   *
   * @param id the document id which is used to uniquely identify it.
   * @return a {@link GetResult} once the document has been loaded.
   */
  public Optional<GetResult> get(final String id) {
    return block(async().get(id));
  }

  /**
   * Fetches a Document (or a fragment of it) from a collection with custom options.
   *
   * <p>The {@link Optional} indicates if the document has been found or not. If the document
   * has not been found, an empty optional will be returned.</p>
   *
   * @param id the document id which is used to uniquely identify it.
   * @param options custom options to change the default behavior.
   * @return a {@link GetResult} once the document has been loaded.
   */
  public Optional<GetResult> get(final String id, final GetOptions options) {
    return block(async().get(id, options));
  }

  /**
   * Removes a Document from a collection with default options.
   *
   * @param id the id of the document to remove.
   * @return a {@link MutationResult} once removed.
   */
  public MutationResult remove(final String id) {
    return block(async().remove(id));
  }

  /**
   * Removes a Document from a collection with custom options.
   *
   * @param id the id of the document to remove.
   * @param options custom options to change the default behavior.
   * @return a {@link MutationResult} once removed.
   */
  public MutationResult remove(final String id, final RemoveOptions options) {
    return block(async().remove(id, options));
  }

  /**
   * Inserts a document fragment into a document which does not exist yet with default options.
   *
   * @param id the unique ID of the document which will be created.
   * @param content the content to be inserted.
   * @return a {@link MutationResult} once inserted.
   */
  public MutationResult insert(final String id, final MutationSpec content) {
    return block(async().insert(id, content));
  }

  /**
   * Inserts a full document which does not exist yet with default options.
   *
   * @param id the unique ID of the document which will be created.
   * @param content the content to be inserted.
   * @param <T> the generic type of the content to be inserted.
   * @return a {@link MutationResult} once inserted.
   */
  public <T> MutationResult insert(final String id, final T content) {
    return block(async().insert(id, content));
  }

  /**
   * Inserts a document fragment into a document which does not exist yet with custom options.
   *
   * @param id the unique ID of the document which will be created.
   * @param spec the content to be inserted.
   * @param options custom options to customize the insert behavior.
   * @return a {@link MutationResult} once inserted.
   */
  public MutationResult insert(final String id, final MutationSpec spec,
                               final InsertOptions options) {
    return block(async().insert(id, spec, options));
  }

  /**
   * Inserts a full document which does not exist yet with custom options.
   *
   * @param id the unique ID of the document which will be created.
   * @param content the content to be inserted.
   * @param options custom options to customize the insert behavior.
   * @param <T> the generic type of the content to be inserted.
   * @return a {@link MutationResult} once inserted.
   */
  public <T> MutationResult insert(final String id, final T content,
                                   final FullInsertOptions<T> options) {
    return block(async().insert(id, content, options));
  }

  /**
   * Helper method to wrap an async call into a blocking one and make sure to
   * convert all checked exceptions into their correct runtime counterparts.
   *
   * @param input the future as input.
   * @param <T> the generic type to return.
   * @return blocks and completes on the given future while converting checked exceptions.
   */
  private static <T> T block(final CompletableFuture<T> input) {
    try {
      return input.get();
    } catch (InterruptedException | ExecutionException e) {
      // todo: figure out if this is the right strategy
      throw new RuntimeException(e);
    }
  }

}
