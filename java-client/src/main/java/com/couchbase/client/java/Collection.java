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

import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.MutateOptions;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.MutateSpec;
import com.couchbase.client.java.kv.GetSpec;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.ReplaceOptions;
import com.couchbase.client.java.kv.UpsertOptions;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.java.AsyncUtils.block;

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

  public Collection(final AsyncCollection asyncCollection, String bucketName) {
    this.asyncCollection = asyncCollection;
    reactiveCollection = new ReactiveCollection(asyncCollection, bucketName);
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
   * Fetches a full Document from a collection with default options.
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
   * Fetches a full Document from a collection with custom options.
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
   * Fetches parts of a document with default options.
   *
   * <p>The {@link Optional} indicates if the document has been found or not. If the document
   * has not been found, an empty optional will be returned.</p>
   *
   * @param id the document id which is used to uniquely identify it.
   * @param spec the spec which allows to configure what should be loaded.
   * @return a {@link GetResult} once the document has been loaded.
   */
  public Optional<GetResult> get(final String id, final GetSpec spec) {
    return block(async().get(id, spec));
  }

  /**
   * Fetches parts of a document with custom options.
   *
   * <p>The {@link Optional} indicates if the document has been found or not. If the document
   * has not been found, an empty optional will be returned.</p>
   *
   * @param id the document id which is used to uniquely identify it.
   * @param spec the spec which allows to configure what should be loaded.
   * @param options custom options to change the default behavior.
   * @return a {@link GetResult} once the document has been loaded.
   */
  public Optional<GetResult> get(final String id, final GetSpec spec, final GetOptions options) {
    return block(async().get(id, spec, options));
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
   * Inserts a full document which does not exist yet with default options.
   *
   * @param id the document id to insert.
   * @param content the document content to insert.
   * @return a {@link MutationResult} once inserted.
   */
  public MutationResult insert(final String id, final Object content) {
    return block(async().insert(id, content));
  }

  /**
   * Inserts a full document which does not exist yet with custom options.
   *
   * @param id the document id to insert.
   * @param content the document content to insert.
   * @param options custom options to customize the insert behavior.
   * @return a {@link MutationResult} once inserted.
   */
  public MutationResult insert(final String id, final Object content, final InsertOptions options) {
    return block(async().insert(id, content, options));
  }

  /**
   * Upserts a full document which might or might not exist yet with default options.
   *
   * @param id the document id to upsert.
   * @param content the document content to upsert.
   * @return a {@link MutationResult} once upserted.
   */
  public MutationResult upsert(final String id, final Object content) {
    return block(async().upsert(id, content));
  }

  /**
   * Upserts a full document which might or might not exist yet with custom options.
   *
   * @param id the document id to upsert.
   * @param content the document content to upsert.
   * @param options custom options to customize the upsert behavior.
   * @return a {@link MutationResult} once upserted.
   */
  public MutationResult upsert(final String id, final Object content, final UpsertOptions options) {
    return block(async().upsert(id, content, options));
  }

  /**
   * Replaces a full document which already exists with default options.
   *
   * @param id the document id to replace.
   * @param content the document content to replace.
   * @return a {@link MutationResult} once replaced.
   */
  public MutationResult replace(final String id, final Object content) {
    return block(async().replace(id, content));
  }

  /**
   * Replaces a full document which already exists with custom options.
   *
   * @param id the document id to replace.
   * @param content the document content to replace.
   * @param options custom options to customize the replace behavior.
   * @return a {@link MutationResult} once replaced.
   */
  public MutationResult replace(final String id, final Object content, final ReplaceOptions options) {
    return block(async().replace(id, content, options));
  }

  /**
   * Performs mutations to document fragments with default options.
   *
   * @param id the outer document ID.
   * @param spec the spec which specifies the type of mutations to perform.
   * @return the {@link MutationResult} once the mutation has been performed or failed.
   */
  public MutationResult mutate(final String id, final MutateSpec spec) {
    return block(async().mutate(id, spec));
  }

  /**
   * Performs mutations to document fragments with custom options.
   *
   * @param id the outer document ID.
   * @param spec the spec which specifies the type of mutations to perform.
   * @param options custom options to modify the mutation options.
   * @return the {@link MutationResult} once the mutation has been performed or failed.
   */
  public MutationResult mutate(final String id, final MutateSpec spec,
                                 final MutateOptions options) {
    return block(async().mutate(id, spec, options));
  }

}
