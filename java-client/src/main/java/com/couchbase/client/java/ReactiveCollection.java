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
import com.couchbase.client.core.Reactor;
import com.couchbase.client.core.msg.kv.GetRequest;
import com.couchbase.client.core.msg.kv.InsertRequest;
import com.couchbase.client.core.msg.kv.RemoveRequest;
import com.couchbase.client.core.msg.kv.ReplaceRequest;
import com.couchbase.client.core.msg.kv.UpsertRequest;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.kv.EncodedDocument;
import com.couchbase.client.java.kv.GetAccessor;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.GetSpec;
import com.couchbase.client.java.kv.InsertAccessor;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.MutateOptions;
import com.couchbase.client.java.kv.MutateSpec;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.RemoveAccessor;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.ReplaceAccessor;
import com.couchbase.client.java.kv.ReplaceOptions;
import com.couchbase.client.java.kv.UpsertAccessor;
import com.couchbase.client.java.kv.UpsertOptions;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;

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

  /**
   * Holds the underlying async collection.
   */
  private final AsyncCollection asyncCollection;

  /**
   * Holds the core context of the attached core.
   */
  private final CoreContext coreContext;

  /**
   * Holds the environment for this collection.
   */
  private final ClusterEnvironment environment;

  /**
   * Holds a direct reference to the core.
   */
  private final Core core;

  private final String bucketName;

  private final byte[] encodedId;

  ReactiveCollection(final AsyncCollection asyncCollection, final String bucketName) {
    this.asyncCollection = asyncCollection;
    this.coreContext = asyncCollection.core().context();
    this.environment = asyncCollection.environment();
    this.core = asyncCollection.core();
    this.bucketName = bucketName;
    this.encodedId = asyncCollection.encodedId();
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
   * Fetches a Document from a collection with default options.
   *
   * <p>If the document has not been found, this {@link Mono} will be empty.</p>
   *
   * @param id the document id which is used to uniquely identify it.
   * @return a {@link Mono} indicating once loaded or failed.
   */
  public Mono<GetResult> get(final String id) {
    return get(id, GetOptions.DEFAULT);
  }

  /**
   * Fetches a Document from a collection with custom options.
   *
   * <p>If the document has not been found, this {@link Mono} will be empty.</p>
   *
   * @param id the document id which is used to uniquely identify it.
   * @param options custom options to change the default behavior.
   * @return a {@link Mono} indicating once loaded or failed.
   */
  public Mono<GetResult> get(final String id, final GetOptions options) {
    notNullOrEmpty(id, "Id");
    notNull(options, "GetOptions");

    return Mono.defer(() -> {
      Duration timeout = Optional.ofNullable(options.timeout()).orElse(environment.kvTimeout());
      RetryStrategy retryStrategy = options.retryStrategy() == null
        ? environment.retryStrategy()
        : options.retryStrategy();
      GetRequest request = new GetRequest(id, encodedId, timeout, coreContext, bucketName, retryStrategy);
      return Reactor
        .wrap(request, GetAccessor.get(core, id, request), true)
        .flatMap(getResult -> getResult.map(Mono::just).orElseGet(Mono::empty));
    });
  }

  /**
   * Fetches parts of a document with default options.
   *
   * <p>If the document has not been found, this {@link Mono} will be empty.</p>
   *
   * @param id the document id which is used to uniquely identify it.
   * @param spec the spec which allows to configure what should be loaded.
   * @return a {@link Mono} indicating once loaded or failed.
   */
  public Mono<GetResult> get(final String id, final GetSpec spec) {
    return get(id, spec, GetOptions.DEFAULT);
  }

  /**
   * Fetches parts of a document with custom options.
   *
   * <p>If the document has not been found, this {@link Mono} will be empty.</p>
   *
   * @param id the document id which is used to uniquely identify it.
   * @param spec the spec which allows to configure what should be loaded.
   * @param options custom options to change the default behavior.
   * @return a {@link Mono} indicating once loaded or failed.
   */
  public Mono<GetResult> get(final String id, final GetSpec spec, final GetOptions options) {
    notNullOrEmpty(id, "Id");
    notNull(spec, "GetSpec");
    notNull(options, "GetOptions");

    throw new UnsupportedOperationException("Implement me -> subdoc get");
  }

  /**
   * Removes a Document from a collection with default options.
   *
   * @param id the id of the document to remove.
   * @return a {@link Mono} completing once removed or failed.
   */
  public Mono<MutationResult> remove(final String id) {
    return remove(id, RemoveOptions.DEFAULT);
  }

  /**
   * Removes a Document from a collection with custom options.
   *
   * @param id the id of the document to remove.
   * @param options custom options to change the default behavior.
   * @return a {@link Mono} completing once removed or failed.
   */
  public Mono<MutationResult> remove(final String id, final RemoveOptions options) {
    notNullOrEmpty(id, "Id");
    notNull(options, "RemoveOptions");

    return Mono.defer(() -> {
      Duration timeout = Optional.ofNullable(options.timeout()).orElse(environment.kvTimeout());
      RetryStrategy retryStrategy = options.retryStrategy() == null
        ? environment.retryStrategy()
        : options.retryStrategy();
      RemoveRequest request = new RemoveRequest(id, encodedId, options.cas(), timeout, coreContext,
        bucketName, retryStrategy);
      return Reactor.wrap(request, RemoveAccessor.remove(core, request), true);
    });
  }


  /**
   * Inserts a full document which does not exist yet with default options.
   *
   * @param id the document id to insert.
   * @param content the document content to insert.
   * @return a {@link Mono} completing once inserted or failed.
   */
  public Mono<MutationResult> insert(final String id, Object content) {
    return insert(id, content, InsertOptions.DEFAULT);
  }

  /**
   * Inserts a full document which does not exist yet with custom options.
   *
   * @param id the document id to insert.
   * @param content the document content to insert.
   * @param options custom options to customize the insert behavior.
   * @return a {@link Mono} completing once inserted or failed.
   */
  public Mono<MutationResult> insert(final String id, Object content,
                                     final InsertOptions options) {
    notNullOrEmpty(id, "Id");
    notNull(content, "Content");
    notNull(options, "InsertOptions");

    return Mono.defer(() -> {
      EncodedDocument encoded = options.encoder().encode(content);
      RetryStrategy retryStrategy = options.retryStrategy() == null
        ? environment.retryStrategy()
        : options.retryStrategy();

      InsertRequest request = new InsertRequest(
        id,
        encodedId,
        encoded.content(),
        options.expiry().getSeconds(),
        encoded.flags(),
        Optional.ofNullable(options.timeout()).orElse(environment.kvTimeout()),
        coreContext,
        bucketName,
        retryStrategy
      );
      return Reactor.wrap(request, InsertAccessor.insert(core, request), true);
    });
  }

  /**
   * Upserts a full document which might or might not exist yet with default options.
   *
   * @param id the document id to upsert.
   * @param content the document content to upsert.
   * @return a {@link Mono} completing once upserted or failed.
   */
  public Mono<MutationResult> upsert(final String id, Object content) {
    return upsert(id, content, UpsertOptions.DEFAULT);
  }

  /**
   * Upserts a full document which might or might not exist yet with custom options.
   *
   * @param id the document id to upsert.
   * @param content the document content to upsert.
   * @param options custom options to customize the upsert behavior.
   * @return a {@link Mono} completing once upserted or failed.
   */
  public Mono<MutationResult> upsert(final String id, Object content,
                                                  final UpsertOptions options) {
    notNullOrEmpty(id, "Id");
    notNull(content, "Content");
    notNull(options, "UpsertOptions");

    return Mono.defer(() -> {
      EncodedDocument encoded = options.encoder().encode(content);
      RetryStrategy retryStrategy = options.retryStrategy() == null
        ? environment.retryStrategy()
        : options.retryStrategy();

      UpsertRequest request = new UpsertRequest(
        id,
        encodedId,
        encoded.content(),
        options.expiry().getSeconds(),
        encoded.flags(),
        Optional.ofNullable(options.timeout()).orElse(environment.kvTimeout()),
        coreContext,
        bucketName,
        retryStrategy
      );
      return Reactor.wrap(request, UpsertAccessor.upsert(core, request), true);
    });
  }

  /**
   * Replaces a full document which already exists with default options.
   *
   * @param id the document id to replace.
   * @param content the document content to replace.
   * @return a {@link Mono} completing once replaced or failed.
   */
  public Mono<MutationResult> replace(final String id, Object content) {
    return replace(id, content, ReplaceOptions.DEFAULT);
  }

  /**
   * Replaces a full document which already exists with custom options.
   *
   * @param id the document id to replace.
   * @param content the document content to replace.
   * @param options custom options to customize the replace behavior.
   * @return a {@link Mono} completing once replaced or failed.
   */
  public Mono<MutationResult> replace(final String id, Object content,
                                                   final ReplaceOptions options) {
    notNullOrEmpty(id, "Id");
    notNull(content, "Content");
    notNull(options, "ReplaceOptions");

    return Mono.defer(() -> {
      EncodedDocument encoded = options.encoder().encode(content);
      RetryStrategy retryStrategy = options.retryStrategy() == null
        ? environment.retryStrategy()
        : options.retryStrategy();

      ReplaceRequest request = new ReplaceRequest(
        id,
        encodedId,
        encoded.content(),
        options.expiry().getSeconds(),
        encoded.flags(),
        Optional.ofNullable(options.timeout()).orElse(environment.kvTimeout()),
        options.cas(),
        coreContext,
        bucketName,
        retryStrategy
      );
      return Reactor.wrap(request, ReplaceAccessor.replace(core, request), true);
    });
  }

  /**
   * Performs mutations to document fragments with default options.
   *
   * @param id the outer document ID.
   * @param spec the spec which specifies the type of mutations to perform.
   * @return the {@link MutationResult} once the mutation has been performed or failed.
   */
  public Mono<MutationResult> mutate(final String id, final MutateSpec spec) {
    return mutate(id, spec, MutateOptions.DEFAULT);
  }

  /**
   * Performs mutations to document fragments with custom options.
   *
   * @param id the outer document ID.
   * @param spec the spec which specifies the type of mutations to perform.
   * @param options custom options to modify the mutation options.
   * @return the {@link MutationResult} once the mutation has been performed or failed.
   */
  public Mono<MutationResult> mutate(final String id, final MutateSpec spec,
                                                  final MutateOptions options) {
    notNullOrEmpty(id, "Id");
    notNull(spec, "MutateSpec");
    notNull(options, "MutateOptions");

    throw new UnsupportedOperationException("Implement me -> subdoc mutate");
  }


}
