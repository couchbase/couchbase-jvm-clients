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
import com.couchbase.client.core.msg.kv.GetRequest;
import com.couchbase.client.core.msg.kv.InsertRequest;
import com.couchbase.client.core.msg.kv.RemoveRequest;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.kv.EncodedDocument;
import com.couchbase.client.java.kv.GetAccessor;
import com.couchbase.client.java.kv.ReadResult;
import com.couchbase.client.java.kv.InsertAccessor;
import com.couchbase.client.java.kv.MutationOptions;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.MutationSpec;
import com.couchbase.client.java.kv.ReadSpec;
import com.couchbase.client.java.kv.RemoveAccessor;
import com.couchbase.client.java.kv.ReadOptions;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.ReplaceOptions;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

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
  private final CouchbaseEnvironment environment;

  /**
   * The name of the collection.
   */
  private final String name;

  /**
   * The scope of the collection.
   */
  private final String scope;

  /**
   * Creates a new {@link AsyncCollection}.
   *
   * @param name the name of the collection.
   * @param scope the scope of the collection.
   * @param core the core into which ops are dispatched.
   * @param environment the surrounding environment for config options.
   */
  public AsyncCollection(final String name, final String scope, final Core core,
                  final CouchbaseEnvironment environment) {
    this.name = name;
    this.scope = scope;
    this.core = core;
    this.coreContext = core.context();
    this.environment = environment;
  }

  /**
   * Provides access to the underlying {@link Core}.
   */
  Core core() {
    return core;
  }

  /**
   * Provides access to the underlying {@link CouchbaseEnvironment}.
   */
  CouchbaseEnvironment environment() {
    return environment;
  }

  /**
   * Fetches a full Document from a collection with default options.
   *
   * <p>The {@link Optional} indicates if the document has been found or not. If the document
   * has not been found, an empty optional will be returned.</p>
   *
   * @param id the document id which is used to uniquely identify it.
   * @return a {@link CompletableFuture} indicating once loaded or failed.
   */
  public CompletableFuture<Optional<ReadResult>> read(final String id) {
    return read(id, ReadSpec.FULL_DOC, ReadOptions.DEFAULT);
  }

  /**
   * Fetches a full Document from a collection with custom options.
   *
   * <p>The {@link Optional} indicates if the document has been found or not. If the document
   * has not been found, an empty optional will be returned.</p>
   *
   * @param id the document id which is used to uniquely identify it.
   * @param options custom options to change the default behavior.
   * @return a {@link CompletableFuture} completing once loaded or failed.
   */
  public CompletableFuture<Optional<ReadResult>> read(final String id, final ReadOptions options) {
    return read(id, ReadSpec.FULL_DOC, options);
  }

  public CompletableFuture<Optional<ReadResult>> read(final String id, final ReadSpec spec) {
    return read(id, spec, ReadOptions.DEFAULT);
  }

  public CompletableFuture<Optional<ReadResult>> read(final String id, final ReadSpec spec,
                                                      final ReadOptions options) {
    notNullOrEmpty(id, "Id");
    notNull(options, "GetOptions");

    // TODO: fold subdoc in here
    if (options.withExpiration()) {
      throw new UnsupportedOperationException("this needs a subdoc fetch, not implemented yet.");
    } else {
      Duration timeout = Optional.ofNullable(options.timeout()).orElse(environment.kvTimeout());
      GetRequest request = new GetRequest(id, timeout, coreContext);
      return GetAccessor.get(core, id, request);
    }
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
    notNullOrEmpty(id, "Id");
    notNull(options, "RemoveOptions");

    Duration timeout = Optional.ofNullable(options.timeout()).orElse(environment.kvTimeout());
    RemoveRequest request = new RemoveRequest(id, options.cas(), timeout, coreContext);
    return RemoveAccessor.remove(core, request);
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
    notNullOrEmpty(id, "Id");
    notNull(content, "Content");
    notNull(options, "InsertOptions");

    EncodedDocument encoded = options.encoder().encode(content);
    InsertRequest request = new InsertRequest(
      id,
      encoded.content(),
      options.expiry().getSeconds(),
      encoded.flags(),
      Optional.ofNullable(options.timeout()).orElse(environment.kvTimeout()),
      coreContext
    );

    return InsertAccessor.insert(core, request);
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
    notNullOrEmpty(id, "Id");
    notNull(content, "Content");
    notNull(options, "ReplaceOptions");

    // TODO:
    return null;
  }

  /**
   * Performs mutations to document fragments with default options.
   *
   * @param id the outer document ID.
   * @param spec the spec which specifies the type of mutations to perform.
   * @return the {@link MutationResult} once the mutation has been performed or failed.
   */
  public CompletableFuture<MutationResult> mutateIn(final String id, final MutationSpec spec) {
    return mutateIn(id, spec, MutationOptions.DEFAULT);
  }

  /**
   * Performs mutations to document fragments with custom options.
   *
   * @param id the outer document ID.
   * @param spec the spec which specifies the type of mutations to perform.
   * @param options custom options to modify the mutation options.
   * @return the {@link MutationResult} once the mutation has been performed or failed.
   */
  public CompletableFuture<MutationResult> mutateIn(final String id, final MutationSpec spec,
                                                    final MutationOptions options) {
    notNullOrEmpty(id, "Id");
    notNull(spec, "MutationSpec");
    notNull(options, "MutationOptions");

    // TODO: fixme.. this is just mocking
    CompletableFuture<MutationResult> f = new CompletableFuture<>();
    f.complete(new MutationResult(0, Optional.empty()));
    return f;
  }

}
