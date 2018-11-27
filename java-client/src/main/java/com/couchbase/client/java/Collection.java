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

import com.couchbase.client.java.kv.Document;
import com.couchbase.client.java.kv.LookupOptions;
import com.couchbase.client.java.kv.LookupSpec;
import com.couchbase.client.java.kv.MutationOptions;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.MutationSpec;
import com.couchbase.client.java.kv.RemoveOptions;

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

  public Collection(final AsyncCollection asyncCollection) {
    this.asyncCollection = asyncCollection;
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
   * @return a {@link Document} once the document has been loaded.
   */
  public Optional<Document> get(final String id) {
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
   * @return a {@link Document} once the document has been loaded.
   */
  public Optional<Document> get(final String id, final GetOptions options) {
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
   * Inserts a full document which does not exist yet with default options.
   *
   * @param document the document to insert.
   * @return a {@link MutationResult} once inserted.
   */
  public MutationResult insert(final Document document) {
    return block(async().insert(document));
  }

  /**
   * Inserts a full document which does not exist yet with custom options.
   *
   * @param document the document to insert.
   * @param options custom options to customize the insert behavior.
   * @return a {@link MutationResult} once inserted.
   */
  public MutationResult insert(final Document document, final InsertOptions options) {
    return block(async().insert(document, options));
  }

  /**
   * Performs mutations to document fragments with default options.
   *
   * @param id the outer document ID.
   * @param spec the spec which specifies the type of mutations to perform.
   * @return the {@link MutationResult} once the mutation has been performed or failed.
   */
  public MutationResult mutateIn(final String id, final MutationSpec spec) {
    return block(async().mutateIn(id, spec));
  }

  /**
   * Performs mutations to document fragments with custom options.
   *
   * @param id the outer document ID.
   * @param spec the spec which specifies the type of mutations to perform.
   * @param options custom options to modify the mutation options.
   * @return the {@link MutationResult} once the mutation has been performed or failed.
   */
  public MutationResult mutateIn(final String id, final MutationSpec spec,
                                 final MutationOptions options) {
    return block(async().mutateIn(id, spec, options));
  }

  /**
   * Performs document fragment lookups with default options.
   *
   * @param id the outer document ID.
   * @param spec the spec which specifies the fields to look up and how.
   * @return a {@link Document} with the fetched fragments and metadata.
   */
  public Document lookupIn(final String id, final LookupSpec spec) {
    return block(async().lookupIn(id, spec));
  }

  /**
   * Performs document fragment lookups with custom options.
   *
   * @param id the outer document ID.
   * @param spec the spec which specifies the fields to look up and how.
   * @param options custom options to modify the lookup options.
   * @return a {@link Document} with the fetched fragments and metadata.
   */
  public Document lookupIn(final String id, final LookupSpec spec, final LookupOptions options) {
    return block(async().lookupIn(id, spec, options));
  }

}
