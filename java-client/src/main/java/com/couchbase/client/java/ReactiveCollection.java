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
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.cnc.events.request.IndividualReplicaGetFailedEvent;
import com.couchbase.client.core.error.context.ReducedKeyValueErrorContext;
import com.couchbase.client.core.msg.kv.GetAndLockRequest;
import com.couchbase.client.core.msg.kv.GetAndTouchRequest;
import com.couchbase.client.core.msg.kv.GetMetaRequest;
import com.couchbase.client.core.msg.kv.GetRequest;
import com.couchbase.client.core.msg.kv.InsertRequest;
import com.couchbase.client.core.msg.kv.RemoveRequest;
import com.couchbase.client.core.msg.kv.ReplaceRequest;
import com.couchbase.client.core.msg.kv.ReplicaGetRequest;
import com.couchbase.client.core.msg.kv.SubdocGetRequest;
import com.couchbase.client.core.msg.kv.TouchRequest;
import com.couchbase.client.core.msg.kv.UnlockRequest;
import com.couchbase.client.core.msg.kv.UpsertRequest;
import com.couchbase.client.java.codec.JsonSerializer;
import com.couchbase.client.java.codec.Transcoder;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.kv.ExistsAccessor;
import com.couchbase.client.java.kv.ExistsOptions;
import com.couchbase.client.java.kv.ExistsResult;
import com.couchbase.client.java.kv.Expiry;
import com.couchbase.client.java.kv.GetAccessor;
import com.couchbase.client.java.kv.GetAllReplicasOptions;
import com.couchbase.client.java.kv.GetAndLockOptions;
import com.couchbase.client.java.kv.GetAndTouchOptions;
import com.couchbase.client.java.kv.GetAnyReplicaOptions;
import com.couchbase.client.java.kv.GetOptions;
import com.couchbase.client.java.kv.GetReplicaResult;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.InsertAccessor;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.LookupInAccessor;
import com.couchbase.client.java.kv.LookupInOptions;
import com.couchbase.client.java.kv.LookupInResult;
import com.couchbase.client.java.kv.LookupInSpec;
import com.couchbase.client.java.kv.MutateInAccessor;
import com.couchbase.client.java.kv.MutateInOptions;
import com.couchbase.client.java.kv.MutateInResult;
import com.couchbase.client.java.kv.MutateInSpec;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.RemoveAccessor;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.ReplaceAccessor;
import com.couchbase.client.java.kv.ReplaceOptions;
import com.couchbase.client.java.kv.StoreSemantics;
import com.couchbase.client.java.kv.TouchAccessor;
import com.couchbase.client.java.kv.TouchOptions;
import com.couchbase.client.java.kv.UnlockAccessor;
import com.couchbase.client.java.kv.UnlockOptions;
import com.couchbase.client.java.kv.UpsertAccessor;
import com.couchbase.client.java.kv.UpsertOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

import static com.couchbase.client.core.util.Validators.notNull;
import static com.couchbase.client.core.util.Validators.notNullOrEmpty;
import static com.couchbase.client.java.kv.ExistsOptions.existsOptions;
import static com.couchbase.client.java.kv.GetAllReplicasOptions.getAllReplicasOptions;
import static com.couchbase.client.java.kv.GetAndLockOptions.getAndLockOptions;
import static com.couchbase.client.java.kv.GetAndTouchOptions.getAndTouchOptions;
import static com.couchbase.client.java.kv.GetAnyReplicaOptions.getAnyReplicaOptions;
import static com.couchbase.client.java.kv.GetOptions.getOptions;
import static com.couchbase.client.java.kv.InsertOptions.insertOptions;
import static com.couchbase.client.java.kv.LookupInOptions.lookupInOptions;
import static com.couchbase.client.java.kv.MutateInOptions.mutateInOptions;
import static com.couchbase.client.java.kv.RemoveOptions.removeOptions;
import static com.couchbase.client.java.kv.ReplaceOptions.replaceOptions;
import static com.couchbase.client.java.kv.TouchOptions.touchOptions;
import static com.couchbase.client.java.kv.UnlockOptions.unlockOptions;
import static com.couchbase.client.java.kv.UpsertOptions.upsertOptions;

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

  static final ExistsOptions DEFAULT_EXISTS_OPTIONS = existsOptions();
  static final GetAndLockOptions DEFAULT_GET_AND_LOCK_OPTIONS = getAndLockOptions();
  static final GetAndTouchOptions DEFAULT_GET_AND_TOUCH_OPTIONS = getAndTouchOptions();
  static final GetAllReplicasOptions DEFAULT_GET_ALL_REPLICAS_OPTIONS = getAllReplicasOptions();
  static final GetAnyReplicaOptions DEFAULT_GET_ANY_REPLICA_OPTIONS = getAnyReplicaOptions();
  static final GetOptions DEFAULT_GET_OPTIONS = getOptions();
  static final InsertOptions DEFAULT_INSERT_OPTIONS = insertOptions();
  static final LookupInOptions DEFAULT_LOOKUP_IN_OPTIONS = lookupInOptions();
  static final MutateInOptions DEFAULT_MUTATE_IN_OPTIONS = mutateInOptions();
  static final RemoveOptions DEFAULT_REMOVE_OPTIONS = removeOptions();
  static final ReplaceOptions DEFAULT_REPLACE_OPTIONS = replaceOptions();
  static final TouchOptions DEFAULT_TOUCH_OPTIONS = touchOptions();
  static final UnlockOptions DEFAULT_UNLOCK_OPTIONS = unlockOptions();
  static final UpsertOptions DEFAULT_UPSERT_OPTIONS = upsertOptions();

  /**
   * Holds the underlying async collection.
   */
  private final AsyncCollection asyncCollection;

  /**
   * Holds the core context of the attached core.
   */
  private final CoreContext coreContext;

  /**
   * Holds a direct reference to the core.
   */
  private final Core core;

  /**
   * Holds the related binary collection.
   */
  private final ReactiveBinaryCollection reactiveBinaryCollection;

  ReactiveCollection(final AsyncCollection asyncCollection) {
    this.asyncCollection = asyncCollection;
    this.coreContext = asyncCollection.core().context();
    this.core = asyncCollection.core();
    this.reactiveBinaryCollection = new ReactiveBinaryCollection(core, asyncCollection.binary());
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
   * @return the {@link ReactiveBinaryCollection}.
   */
  public ReactiveBinaryCollection binary() {
    return reactiveBinaryCollection;
  }

  /**
   * Fetches a Document from a collection with default options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @return a {@link Mono} indicating once loaded or failed.
   */
  public Mono<GetResult> get(final String id) {
    return get(id, DEFAULT_GET_OPTIONS);
  }

  /**
   * Fetches a Document from a collection with custom options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param options custom options to change the default behavior.
   * @return a {@link Mono} indicating once loaded or failed
   */
  public Mono<GetResult> get(final String id, final GetOptions options) {
    return Mono.defer(() -> {
      GetOptions.Built opts = options.build();
      final Transcoder transcoder = opts.transcoder() == null ? environment().transcoder() : opts.transcoder();

      if (opts.projections().isEmpty() && !opts.withExpiry()) {
        GetRequest request = asyncCollection.fullGetRequest(id, opts);
        return Reactor.wrap(request, GetAccessor.get(core, request, transcoder), true);
      } else {
        SubdocGetRequest request = asyncCollection.subdocGetRequest(id, opts);
        return Reactor.wrap(request, GetAccessor.subdocGet(core, request, transcoder), true);
      }
    });
  }

  /**
   * Fetches a full document and write-locks it for the given duration with default options.
   * <p>
   * Note that the client does not enforce an upper limit on the {@link Duration} lockTime. The maximum lock time
   * by default on the server is 30 seconds. Any value larger than 30 seconds will be capped down by the server to
   * the default lock time, which is 15 seconds unless modified on the server side.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param lockTime how long to write-lock the document for (any duration > 30s will be capped to server default of 15s).
   * @return a {@link Mono} completing once loaded or failed.
   */
  public Mono<GetResult> getAndLock(final String id, final Duration lockTime) {
    return getAndLock(id, lockTime, DEFAULT_GET_AND_LOCK_OPTIONS);
  }

  /**
   * Fetches a full document and write-locks it for the given duration with custom options.
   * <p>
   * Note that the client does not enforce an upper limit on the {@link Duration} lockTime. The maximum lock time
   * by default on the server is 30 seconds. Any value larger than 30 seconds will be capped down by the server to
   * the default lock time, which is 15 seconds unless modified on the server side.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param lockTime how long to write-lock the document for (any duration > 30s will be capped to server default of 15s).
   * @param options custom options to change the default behavior.
   * @return a {@link Mono} completing once loaded or failed.
   */
  public Mono<GetResult> getAndLock(final String id, final Duration lockTime, final GetAndLockOptions options) {
    return Mono.defer(() -> {
      GetAndLockOptions.Built opts = options.build();
      final Transcoder transcoder = opts.transcoder() == null ? environment().transcoder() : opts.transcoder();
      GetAndLockRequest request = asyncCollection.getAndLockRequest(id, lockTime, opts);
      return Reactor.wrap(request, GetAccessor.getAndLock(core, request, transcoder), true);
    });
  }

  /**
   * Fetches a full document and resets its expiration time to the value provided with default
   * options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param expiry the new expiration time for the document.
   * @return a {@link Mono} completing once loaded or failed.
   */
  public Mono<GetResult> getAndTouch(final String id, final Duration expiry) {
    return getAndTouch(id, expiry, DEFAULT_GET_AND_TOUCH_OPTIONS);
  }

  /**
   * Fetches a full document and resets its expiration time to the value provided with custom
   * options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param expiry the new expiration time for the document.
   * @param options custom options to change the default behavior.
   * @return a {@link Mono} completing once loaded or failed.
   */
  public Mono<GetResult> getAndTouch(final String id, final Duration expiry, final GetAndTouchOptions options) {
    return Mono.defer(() -> {
      GetAndTouchOptions.Built opts = options.build();
      final Transcoder transcoder = opts.transcoder() == null ? environment().transcoder() : opts.transcoder();
      GetAndTouchRequest request = asyncCollection.getAndTouchRequest(id, Expiry.relative(expiry), opts);
      return Reactor.wrap(request, GetAccessor.getAndTouch(core, request, transcoder), true);
    });
  }

  /**
   * Reads all available replicas, including the active, and returns the results as a flux.
   *
   * <p>Note that individual errors are ignored, so you can think of this API as a best effort
   * approach which explicitly emphasises availability over consistency.</p>
   * <p>
   * Raises NoSuchElementException on the flux if no replica was available.
   *
   * @param id the document id.
   * @return a flux of results from all replicas
   */
  public Flux<GetReplicaResult> getAllReplicas(final String id) {
    return getAllReplicas(id, DEFAULT_GET_ALL_REPLICAS_OPTIONS);
  }

  /**
   * Reads all available replicas, including the active, and returns the results as a flux.
   * <p>
   * Raises NoSuchElementException on the flux if no replica was available.
   *
   * @param id the document id.
   * @param options the custom options.
   * @return a flux of results from all replicas
   */
  public Flux<GetReplicaResult> getAllReplicas(final String id, final GetAllReplicasOptions options) {
    notNullOrEmpty(id, "Id", () -> ReducedKeyValueErrorContext.create(id, asyncCollection.collectionIdentifier()));
    notNull(options, "GetAllReplicasOptions", () -> ReducedKeyValueErrorContext.create(id, asyncCollection.collectionIdentifier()));
    GetAllReplicasOptions.Built opts = options.build();
    Duration timeout = opts.timeout().orElse(environment().timeoutConfig().kvTimeout());
    RequestSpan parent = environment().requestTracer().requestSpan(
      TracingIdentifiers.SPAN_GET_ALL_REPLICAS,
      opts.parentSpan().orElse(null)
    );
    parent.setAttribute(TracingIdentifiers.ATTR_SYSTEM, TracingIdentifiers.ATTR_SYSTEM_COUCHBASE);
    final Transcoder transcoder = opts.transcoder() == null ? environment().transcoder() : opts.transcoder();

    return Reactor
      .toMono(() -> asyncCollection.getAllReplicasRequests(id, opts, timeout, parent))
      .flux()
      .flatMap(Flux::fromStream)
      .flatMap(request -> Reactor
        .wrap(request, GetAccessor.get(core, request, transcoder), true)
        .onErrorResume(t -> {
          coreContext.environment().eventBus().publish(new IndividualReplicaGetFailedEvent(request.context()));
          return Mono.empty(); // Swallow any errors from individual replicas
        })
        .map(response -> GetReplicaResult.from(response, request instanceof ReplicaGetRequest))
      )
      .doFinally(signalType -> parent.end());
  }

  /**
   * Reads all available replicas, and returns the first found.
   * <p>
   * Raises NoSuchElementException on the mono if no replica was available.
   *
   * @param id the document id.
   * @return a mono containing the first available replica.
   */
  public Mono<GetReplicaResult> getAnyReplica(final String id) {
    return getAnyReplica(id, DEFAULT_GET_ANY_REPLICA_OPTIONS);
  }

    /**
     * Reads all available replicas, and returns the first found.
     * <p>
     * Raises NoSuchElementException on the mono if no replica was available.
     *
     * @param id the document id.
     * @param options the custom options.
     * @return a mono containing the first available replica.
     */
  public Mono<GetReplicaResult> getAnyReplica(final String id, final GetAnyReplicaOptions options) {
    GetAnyReplicaOptions.Built built = options.build();
    GetAllReplicasOptions opts = GetAllReplicasOptions.getAllReplicasOptions().clientContext(built.clientContext());
    built.timeout().ifPresent(opts::timeout);
    built.retryStrategy().ifPresent(opts::retryStrategy);
    if (built.transcoder() != null) {
      opts.transcoder(built.transcoder());
    }
    RequestSpan parent = environment().requestTracer().requestSpan(
      TracingIdentifiers.SPAN_GET_ANY_REPLICA,
      built.parentSpan().orElse(null)
    );
    opts.parentSpan(parent);
    return getAllReplicas(id, opts).next().doFinally(signalType -> parent.end());
  }

  /**
   * Checks if the given document ID exists on the active partition with default options.
   *
   * @param id the document ID
   * @return a {@link Mono} completing once loaded or failed.
   */
  public Mono<ExistsResult> exists(final String id) {
    return exists(id, DEFAULT_EXISTS_OPTIONS);
  }

  /**
   * Checks if the given document ID exists on the active partition with custom options.
   *
   * @param id the document ID
   * @param options to modify the default behavior
   * @return a {@link Mono} completing once loaded or failed.
   */
  public Mono<ExistsResult> exists(final String id, final ExistsOptions options) {
    return Mono.defer(() -> {
      GetMetaRequest request = asyncCollection.existsRequest(id, options);
      return Reactor
        .wrap(request, ExistsAccessor.exists(id, core, request), true);
    });
  }

  /**
   * Removes a Document from a collection with default options.
   *
   * @param id the id of the document to remove.
   * @return a {@link Mono} completing once removed or failed.
   */
  public Mono<MutationResult> remove(final String id) {
    return remove(id, DEFAULT_REMOVE_OPTIONS);
  }

  /**
   * Removes a Document from a collection with custom options.
   *
   * @param id the id of the document to remove.
   * @param options custom options to change the default behavior.
   * @return a {@link Mono} completing once removed or failed.
   */
  public Mono<MutationResult> remove(final String id, final RemoveOptions options) {
    return Mono.defer(() -> {
      notNull(options, "RemoveOptions", () -> ReducedKeyValueErrorContext.create(id, asyncCollection.collectionIdentifier()));
      RemoveOptions.Built opts = options.build();
      RemoveRequest request = asyncCollection.removeRequest(id, opts);
      return Reactor.wrap(
        request,
        RemoveAccessor.remove(core, request, id, opts.persistTo(), opts.replicateTo()),
        true
      );
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
    return insert(id, content, DEFAULT_INSERT_OPTIONS);
  }

  /**
   * Inserts a full document which does not exist yet with custom options.
   *
   * @param id the document id to insert.
   * @param content the document content to insert.
   * @param options custom options to customize the insert behavior.
   * @return a {@link Mono} completing once inserted or failed.
   */
  public Mono<MutationResult> insert(final String id, Object content, final InsertOptions options) {
    return Mono.defer(() -> {
      notNull(options, "InsertOptions", () -> ReducedKeyValueErrorContext.create(id, asyncCollection.collectionIdentifier()));
      InsertOptions.Built opts = options.build();
      InsertRequest request = asyncCollection.insertRequest(id, content, opts);
      return Reactor.wrap(
        request,
        InsertAccessor.insert(core, request, id, opts.persistTo(), opts.replicateTo()),
        true
      );
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
    return upsert(id, content, DEFAULT_UPSERT_OPTIONS);
  }

  /**
   * Upserts a full document which might or might not exist yet with custom options.
   *
   * @param id the document id to upsert.
   * @param content the document content to upsert.
   * @param options custom options to customize the upsert behavior.
   * @return a {@link Mono} completing once upserted or failed.
   */
  public Mono<MutationResult> upsert(final String id, Object content, final UpsertOptions options) {
    return Mono.defer(() -> {
      notNull(options, "UpsertOptions", () -> ReducedKeyValueErrorContext.create(id, asyncCollection.collectionIdentifier()));
      UpsertOptions.Built opts = options.build();
      UpsertRequest request = asyncCollection.upsertRequest(id, content, opts);
      return Reactor.wrap(
        request,
        UpsertAccessor.upsert(core, request, id, opts.persistTo(), opts.replicateTo()),
        true
      );
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
    return replace(id, content, DEFAULT_REPLACE_OPTIONS);
  }

  /**
   * Replaces a full document which already exists with custom options.
   *
   * @param id the document id to replace.
   * @param content the document content to replace.
   * @param options custom options to customize the replace behavior.
   * @return a {@link Mono} completing once replaced or failed.
   */
  public Mono<MutationResult> replace(final String id, Object content, final ReplaceOptions options) {
    return Mono.defer(() -> {
      notNull(options, "ReplaceOptions", () -> ReducedKeyValueErrorContext.create(id, asyncCollection.collectionIdentifier()));
      ReplaceOptions.Built opts = options.build();
      ReplaceRequest request = asyncCollection.replaceRequest(id, content, opts);
      return Reactor.wrap(
        request,
        ReplaceAccessor.replace(core, request, id, opts.persistTo(), opts.replicateTo()),
        true
      );
    });
  }

  /**
   * Updates the expiry of the document with the given id with default options.
   *
   * @param id the id of the document to update.
   * @param expiry the new expiry for the document.
   * @return a {@link MutationResult} once the operation completes.
   */
  public Mono<MutationResult> touch(final String id, final Duration expiry) {
    return touch(id, expiry, DEFAULT_TOUCH_OPTIONS);
  }

  /**
   * Updates the expiry of the document with the given id with custom options.
   *
   * @param id the id of the document to update.
   * @param expiry the new expiry for the document.
   * @param options the custom options.
   * @return a {@link MutationResult} once the operation completes.
   */
  public Mono<MutationResult> touch(final String id, final Duration expiry, final TouchOptions options) {
    return Mono.defer(() -> {
      TouchRequest request = asyncCollection.touchRequest(id, Expiry.relative(expiry), options);
      return Reactor.wrap(request, TouchAccessor.touch(core, request, id), true);
    });
  }

  /**
   * Unlocks a document if it has been locked previously, with default options.
   *
   * @param id the id of the document.
   * @param cas the CAS value which is needed to unlock it.
   * @return the mono which completes once a response has been received.
   */
  public Mono<Void> unlock(final String id, final long cas) {
    return unlock(id, cas, DEFAULT_UNLOCK_OPTIONS);
  }

  /**
   * Unlocks a document if it has been locked previously, with custom options.
   *
   * @param id the id of the document.
   * @param cas the CAS value which is needed to unlock it.
   * @param options the options to customize.
   * @return the mono which completes once a response has been received.
   */
  public Mono<Void> unlock(final String id, final long cas, final UnlockOptions options) {
    return Mono.defer(() -> {
      UnlockRequest request = asyncCollection.unlockRequest(id, cas, options);
      return Reactor.wrap(request, UnlockAccessor.unlock(id, core, request), true);
    });
  }

  /**
   * Performs lookups to document fragments with default options.
   *
   * @param id the outer document ID.
   * @param specs the spec which specifies the type of lookups to perform.
   * @return the {@link LookupInResult} once the lookup has been performed or failed.
   */
  public Mono<LookupInResult> lookupIn(final String id, List<LookupInSpec> specs) {
    return lookupIn(id, specs, DEFAULT_LOOKUP_IN_OPTIONS);
  }

  /**
   * Performs lookups to document fragments with custom options.
   *
   * @param id the outer document ID.
   * @param specs the spec which specifies the type of lookups to perform.
   * @param options custom options to modify the lookup options.
   * @return the {@link LookupInResult} once the lookup has been performed or failed.
   */
  public Mono<LookupInResult> lookupIn(final String id, List<LookupInSpec> specs, final LookupInOptions options) {
    return Mono.defer(() -> {
      notNull(options, "LookupInOptions", () -> ReducedKeyValueErrorContext.create(id, asyncCollection.collectionIdentifier()));
      LookupInOptions.Built opts = options.build();
      JsonSerializer serializer = opts.serializer() == null ? environment().jsonSerializer() : opts.serializer();
      SubdocGetRequest request = asyncCollection.lookupInRequest(id, specs, opts);
      return Reactor.wrap(
        request,
        LookupInAccessor.lookupInAccessor(core, request, serializer),
        true
      );
    });
  }

  /**
   * Performs mutations to document fragments with default options.
   *
   * @param id the outer document ID.
   * @param specs the spec which specifies the type of mutations to perform.
   * @return the {@link MutateInResult} once the mutation has been performed or failed.
   */
  public Mono<MutateInResult> mutateIn(final String id, final List<MutateInSpec> specs) {
    return mutateIn(id, specs, DEFAULT_MUTATE_IN_OPTIONS);
  }

  /**
   * Performs mutations to document fragments with custom options.
   *
   * @param id the outer document ID.
   * @param specs the spec which specifies the type of mutations to perform.
   * @param options custom options to modify the mutation options.
   * @return the {@link MutateInResult} once the mutation has been performed or failed.
   */
  public Mono<MutateInResult> mutateIn(final String id, final List<MutateInSpec> specs,
                                       final MutateInOptions options) {
    return Mono.defer(() -> {
      notNull(options, "MutateInOptions", () -> ReducedKeyValueErrorContext.create(id, asyncCollection.collectionIdentifier()));
      MutateInOptions.Built opts = options.build();
      Duration timeout = AsyncCollection.decideKvTimeout(opts, environment().timeoutConfig());

      return Mono.fromFuture(asyncCollection.mutateInRequest(id, specs, opts, timeout))
              .flatMap(request -> Reactor.wrap(request,
                          MutateInAccessor.mutateIn(core, request, id, opts.persistTo(), opts.replicateTo(), opts.storeSemantics() == StoreSemantics.INSERT, environment().jsonSerializer()),
                          true));
    });
  }

}
