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
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.msg.kv.*;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.util.UnsignedLEB128;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.kv.AppendOptions;
import com.couchbase.client.java.kv.CounterOptions;
import com.couchbase.client.java.kv.EncodedDocument;
import com.couchbase.client.java.kv.GetAccessor;
import com.couchbase.client.java.kv.GetAndLockOptions;
import com.couchbase.client.java.kv.GetAndTouchOptions;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.InsertAccessor;
import com.couchbase.client.java.kv.LookupOptions;
import com.couchbase.client.java.kv.LookupResult;
import com.couchbase.client.java.kv.LookupSpec;
import com.couchbase.client.java.kv.MutateOptions;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.MutateSpec;
import com.couchbase.client.java.kv.PrependOptions;
import com.couchbase.client.java.kv.RemoveAccessor;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.ReplaceAccessor;
import com.couchbase.client.java.kv.ReplaceOptions;
import com.couchbase.client.java.kv.TouchOptions;
import com.couchbase.client.java.kv.UnlockOptions;
import com.couchbase.client.java.kv.UpsertAccessor;
import com.couchbase.client.java.kv.UpsertOptions;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

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
  private final ClusterEnvironment environment;

  /**
   * The name of the collection.
   */
  private final String name;

  /**
   * The name of the bucket.
   */
  private final String bucket;

  /**
   * Holds the collection id in an encoded format.
   */
  private final byte[] collectionId;

  /**
   * Creates a new {@link AsyncCollection}.
   *
   * @param name the name of the collection.
   * @param id the id
   * @param core the core into which ops are dispatched.
   * @param environment the surrounding environment for config options.
   */
  public AsyncCollection(final String name, final long id, final String bucket,
                         final Core core, final ClusterEnvironment environment) {
    this.name = name;
    this.core = core;
    this.coreContext = core.context();
    this.environment = environment;
    this.bucket = bucket;
    this.collectionId = UnsignedLEB128.encode(id);
  }

  /**
   * Provides access to the underlying {@link Core}.
   */
  @Stability.Internal
  public Core core() {
    return core;
  }

  /**
   * Provides access to the underlying {@link ClusterEnvironment}.
   */
  public ClusterEnvironment environment() {
    return environment;
  }

  /**
   * Returns the encoded collection id used for KV operations.
   */
  @Stability.Internal
  byte[] collectionId() {
    return collectionId;
  }

  /**
   * The name of the collection in use.
   *
   * @return the name of the collection.
   */
  public String name() {
    return name;
  }

  /**
   * Fetches a full document (or a projection of it) from a collection with default options.
   *
   * <p>The {@link Optional} indicates if the document has been found or not. If the document
   * has not been found, an empty optional will be returned.</p>
   *
   * @param id the document id which is used to uniquely identify it.
   * @return a {@link CompletableFuture} indicating once loaded or failed.
   */
  public CompletableFuture<Optional<GetResult>> get(final String id) {
    return get(id, GetOptions.DEFAULT);
  }

  /**
   * Fetches a full document (or a projection of it) from a collection with custom options.
   *
   * <p>The {@link Optional} indicates if the document has been found or not. If the document
   * has not been found, an empty optional will be returned.</p>
   *
   * @param id the document id which is used to uniquely identify it.
   * @param options custom options to change the default behavior.
   * @return a {@link CompletableFuture} completing once loaded or failed.
   */
  public CompletableFuture<Optional<GetResult>> get(final String id, final GetOptions options) {
    notNullOrEmpty(id, "Id");
    notNull(options, "GetOptions");

    Duration timeout = Optional.ofNullable(options.timeout()).orElse(environment.kvTimeout());
    RetryStrategy retryStrategy = options.retryStrategy() == null
      ? environment.retryStrategy()
      : options.retryStrategy();

    if (options.projections() != null) {
      List<SubdocGetRequest.Command> commands = options
        .projections()
        .stream()
        .filter(s -> s != null && !s.isEmpty())
        .map(s -> new SubdocGetRequest.Command(SubdocGetRequest.CommandType.GET, s, false))
        .collect(Collectors.toList());

      // todo: if over 16 fields, degrade to fulldoc and do the projection in java

      return getProjection(id, options, timeout, retryStrategy, commands);
    } else {
      return getFullDoc(id, options, timeout, retryStrategy);
    }
  }

  private CompletableFuture<Optional<GetResult>> getFullDoc(final String id,
                                                            final GetOptions options,
                                                            final Duration timeout,
                                                            final RetryStrategy retryStrategy) {
    if (options.withExpiration()) {
      List<SubdocGetRequest.Command> command = new ArrayList<>();
      command.add(new SubdocGetRequest.Command(SubdocGetRequest.CommandType.GET_DOC, "", false));
      return getProjection(id, options, timeout, retryStrategy, command);
    }

    GetRequest request = new GetRequest(id, collectionId, timeout, coreContext, bucket, retryStrategy);
    return GetAccessor.get(core, id, request);
  }

  private CompletableFuture<Optional<GetResult>> getProjection(final String id,
                                                               final GetOptions options,
                                                               final Duration timeout,
                                                               final RetryStrategy retryStrategy,
                                                               final List<SubdocGetRequest.Command> commands) {
    if (options.withExpiration()) {
      commands.add(0, new SubdocGetRequest.Command(
        SubdocGetRequest.CommandType.GET,
        "$document.exptime",
        true)
      );
    }

    SubdocGetRequest request = new SubdocGetRequest(timeout, coreContext, bucket, retryStrategy,
      id, collectionId, (byte) 0, commands);
    return GetAccessor.subdocGet(core, id, request);
  }

  /**
   * Fetches a full document and write-locks it for the given duration with default options.
   *
   * <p>The {@link Optional} indicates if the document has been found or not. If the document
   * has not been found, an empty optional will be returned.</p>
   *
   * @param id the document id which is used to uniquely identify it.
   * @param lockFor the duration the write lock should be automatically released after.
   * @return a {@link CompletableFuture} completing once loaded or failed.
   */
  public CompletableFuture<Optional<GetResult>> getAndLock(final String id, final Duration lockFor) {
    return getAndLock(id, lockFor, GetAndLockOptions.DEFAULT);
  }

  /**
   * Fetches a full document and write-locks it for the given duration with custom options.
   *
   * <p>The {@link Optional} indicates if the document has been found or not. If the document
   * has not been found, an empty optional will be returned.</p>
   *
   * @param id the document id which is used to uniquely identify it.
   * @param lockFor the duration the write lock should be automatically released after.
   * @param options custom options to change the default behavior.
   * @return a {@link CompletableFuture} completing once loaded or failed.
   */
  public CompletableFuture<Optional<GetResult>> getAndLock(final String id, final Duration lockFor,
                                                           final GetAndLockOptions options) {
    notNullOrEmpty(id, "Id");
    notNull(lockFor, "LockTime");
    notNull(options, "GetAndLockOptions");

    Duration timeout = Optional.ofNullable(options.timeout()).orElse(environment.kvTimeout());
    RetryStrategy retryStrategy = options.retryStrategy() == null
      ? environment.retryStrategy()
      : options.retryStrategy();
    GetAndLockRequest request = new GetAndLockRequest(id, collectionId, timeout, coreContext, bucket, retryStrategy,
      lockFor);
    return GetAccessor.getAndLock(core, id, request);
  }

  /**
   * Fetches a full document and resets its expiration time to the value provided with default options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param expiration the new expiration time for the document.
   * @return a {@link CompletableFuture} completing once loaded or failed.
   */
  public CompletableFuture<Optional<GetResult>> getAndTouch(final String id,
                                                            final Duration expiration) {
    return getAndTouch(id, expiration, GetAndTouchOptions.DEFAULT);
  }

  /**
   * Fetches a full document and resets its expiration time to the value provided with default options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param expiration the new expiration time for the document.
   * @param options custom options to change the default behavior.
   * @return a {@link CompletableFuture} completing once loaded or failed.
   */
  public CompletableFuture<Optional<GetResult>> getAndTouch(final String id,
                                                            final Duration expiration,
                                                            final GetAndTouchOptions options) {
    notNullOrEmpty(id, "Id");
    notNull(expiration, "Expiration");
    notNull(options, "GetAndTouchOptions");

    Duration timeout = Optional.ofNullable(options.timeout()).orElse(environment.kvTimeout());
    RetryStrategy retryStrategy = options.retryStrategy() == null
      ? environment.retryStrategy()
      : options.retryStrategy();
    GetAndTouchRequest request = new GetAndTouchRequest(id, collectionId, timeout, coreContext, bucket, retryStrategy,
      expiration);
    return GetAccessor.getAndTouch(core, id, request);  }


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
    RetryStrategy retryStrategy = options.retryStrategy() == null
      ? environment.retryStrategy()
      : options.retryStrategy();
    RemoveRequest request = new RemoveRequest(id, collectionId, options.cas(), timeout,
      coreContext, bucket, retryStrategy);
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
    RetryStrategy retryStrategy = options.retryStrategy() == null
      ? environment.retryStrategy()
      : options.retryStrategy();

    InsertRequest request = new InsertRequest(
      id,
            collectionId,
      encoded.content(),
      options.expiry().getSeconds(),
      encoded.flags(),
      Optional.ofNullable(options.timeout()).orElse(environment.kvTimeout()),
      coreContext,
      bucket,
      retryStrategy
    );

    return InsertAccessor.insert(core, request);
  }

  /**
   * Upserts a full document which might or might not exist yet with default options.
   *
   * @param id the document id to upsert.
   * @param content the document content to upsert.
   * @return a {@link CompletableFuture} completing once upserted or failed.
   */
  public CompletableFuture<MutationResult> upsert(final String id, Object content) {
    return upsert(id, content, UpsertOptions.DEFAULT);
  }

  /**
   * Upserts a full document which might or might not exist yet with custom options.
   *
   * @param id the document id to upsert.
   * @param content the document content to upsert.
   * @param options custom options to customize the upsert behavior.
   * @return a {@link CompletableFuture} completing once upserted or failed.
   */
  public CompletableFuture<MutationResult> upsert(final String id, Object content,
                                                  final UpsertOptions options) {
    notNullOrEmpty(id, "Id");
    notNull(content, "Content");
    notNull(options, "UpsertOptions");

    EncodedDocument encoded = options.encoder().encode(content);
    RetryStrategy retryStrategy = options.retryStrategy() == null
      ? environment.retryStrategy()
      : options.retryStrategy();

    UpsertRequest request = new UpsertRequest(
      id,
            collectionId,
      encoded.content(),
      options.expiry().getSeconds(),
      encoded.flags(),
      Optional.ofNullable(options.timeout()).orElse(environment.kvTimeout()),
      coreContext,
      bucket,
      retryStrategy
    );

    return UpsertAccessor.upsert(core, request);
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

    EncodedDocument encoded = options.encoder().encode(content);
    RetryStrategy retryStrategy = options.retryStrategy() == null
      ? environment.retryStrategy()
      : options.retryStrategy();

    ReplaceRequest request = new ReplaceRequest(
      id,
            collectionId,
      encoded.content(),
      options.expiry().getSeconds(),
      encoded.flags(),
      Optional.ofNullable(options.timeout()).orElse(environment.kvTimeout()),
      options.cas(),
      coreContext,
      bucket,
      retryStrategy
    );
    return ReplaceAccessor.replace(core, request);
  }

  public CompletableFuture<MutationResult> append(final String id, final byte[] content) {
    return append(id, content, AppendOptions.DEFAULT);
  }

  public CompletableFuture<MutationResult> append(final String id, final byte[] content,
                                                  final AppendOptions options) {
    notNullOrEmpty(id, "Id");
    notNull(content, "Content");
    notNull(options, "AppendOptions");

    return null;
  }

  public CompletableFuture<MutationResult> prepend(final String id, final byte[] content) {
    return prepend(id, content, PrependOptions.DEFAULT);
  }

  public CompletableFuture<MutationResult> prepend(final String id, final byte[] content,
                                                  final PrependOptions options) {
    notNullOrEmpty(id, "Id");
    notNull(content, "Content");
    notNull(options, "PrependOptions");

    return null;
  }

  public CompletableFuture<Void> touch(final String id) {
    return touch(id, TouchOptions.DEFAULT);
  }

  public CompletableFuture<Void> touch(final String id, final TouchOptions options) {
    notNullOrEmpty(id, "Id");
    notNull(options, "TouchOptions");

    return null;
  }

  public CompletableFuture<MutationResult> unlock(final String id) {
    return unlock(id, UnlockOptions.DEFAULT);
  }

  public CompletableFuture<MutationResult> unlock(final String id, final UnlockOptions options) {
    notNullOrEmpty(id, "Id");
    notNull(options, "UnlockOptions");

    return null;
  }

  public CompletableFuture<MutationResult> counter(final String id, long delta) {
    return counter(id, delta, CounterOptions.DEFAULT);
  }

  public CompletableFuture<MutationResult> counter(final String id, long delta,
                                                   final CounterOptions options) {
    notNullOrEmpty(id, "Id");
    notNull(options, "CounterOptions");

    return null;
  }

  public CompletableFuture<Optional<LookupResult>> lookupIn(final String id, final LookupSpec spec) {
    return lookupIn(id, spec, LookupOptions.DEFAULT);
  }

  public CompletableFuture<Optional<LookupResult>> lookupIn(final String id, final LookupSpec spec,
                                                            final LookupOptions options) {
    notNullOrEmpty(id, "Id");
    notNull(spec, "LookupSpec");
    notNull(options, "LookupOptions");

    return null;
  }

  /**
   * Performs mutations to document fragments with default options.
   *
   * @param id the outer document ID.
   * @param spec the spec which specifies the type of mutations to perform.
   * @return the {@link MutationResult} once the mutation has been performed or failed.
   */
  public CompletableFuture<MutationResult> mutateIn(final String id, final MutateSpec spec) {
    return mutateIn(id, spec, MutateOptions.DEFAULT);
  }

  /**
   * Performs mutations to document fragments with custom options.
   *
   * @param id the outer document ID.
   * @param spec the spec which specifies the type of mutations to perform.
   * @param options custom options to modify the mutation options.
   * @return the {@link MutationResult} once the mutation has been performed or failed.
   */
  public CompletableFuture<MutationResult> mutateIn(final String id, final MutateSpec spec,
                                                    final MutateOptions options) {
    notNullOrEmpty(id, "Id");
    notNull(spec, "MutateSpec");
    notNull(options, "MutateOptions");

    throw new UnsupportedOperationException("Implement me -> subdoc mutateIn");
  }

}
