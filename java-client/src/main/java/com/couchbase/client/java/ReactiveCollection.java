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
import com.couchbase.client.core.cnc.events.request.IndividualReplicaGetFailedEvent;
import com.couchbase.client.core.msg.kv.*;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.kv.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

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
  @Stability.Internal
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
    return Mono.defer(() -> {
      GetOptions.BuiltGetOptions opts = options.build();
      if (opts.projections() == null && !opts.withExpiration()) {
        GetRequest request = asyncCollection.fullGetRequest(id, options);
        return Reactor
          .wrap(request, GetAccessor.get(core, id, request), true)
          .flatMap(getResult -> getResult.map(Mono::just).orElseGet(Mono::empty));
      } else {
        SubdocGetRequest request = asyncCollection.subdocGetRequest(id, options);
        return Reactor
          .wrap(request, GetAccessor.subdocGet(core, id, request), true)
          .flatMap(getResult -> getResult.map(Mono::just).orElseGet(Mono::empty));
      }
    });
  }

  /**
   * Fetches a full document and write-locks it for the given duration with default options.
   *
   * <p>The {@link Optional} indicates if the document has been found or not. If the document
   * has not been found, an empty optional will be returned.</p>
   *
   * @param id the document id which is used to uniquely identify it.
   * @return a {@link Mono} completing once loaded or failed.
   */
  public Mono<GetResult> getAndLock(final String id) {
    return getAndLock(id, GetAndLockOptions.DEFAULT);
  }

  /**
   * Fetches a full document and write-locks it for the given duration with custom options.
   *
   * <p>The {@link Optional} indicates if the document has been found or not. If the document
   * has not been found, an empty optional will be returned.</p>
   *
   * @param id the document id which is used to uniquely identify it.
   * @param options custom options to change the default behavior.
   * @return a {@link Mono} completing once loaded or failed.
   */
  public Mono<GetResult> getAndLock(final String id, final GetAndLockOptions options) {
    return Mono.defer(() -> {
      GetAndLockRequest request = asyncCollection.getAndLockRequest(id, options);
      return Reactor
        .wrap(request, GetAccessor.getAndLock(core, id, request), true)
        .flatMap(getResult -> getResult.map(Mono::just).orElseGet(Mono::empty));
    });
  }

  /**
   * Fetches a full document and resets its expiration time to the value provided with default
   * options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param expiration the new expiration time for the document.
   * @return a {@link Mono} completing once loaded or failed.
   */
  public Mono<GetResult> getAndTouch(final String id, final Duration expiration) {
    return getAndTouch(id, expiration, GetAndTouchOptions.DEFAULT);
  }

  /**
   * Fetches a full document and resets its expiration time to the value provided with custom
   * options.
   *
   * @param id the document id which is used to uniquely identify it.
   * @param expiration the new expiration time for the document.
   * @param options custom options to change the default behavior.
   * @return a {@link Mono} completing once loaded or failed.
   */
  public Mono<GetResult> getAndTouch(final String id, final Duration expiration,
                                     final GetAndTouchOptions options) {
    return Mono.defer(() -> {
      GetAndTouchRequest request = asyncCollection.getAndTouchRequest(id, expiration, options);
      GetAndTouchOptions.BuiltGetAndTouchOptions opts = options.build();
      return Reactor
        .wrap(
          request,
          GetAccessor.getAndTouch(core, id, request, opts.persistTo(), opts.replicateTo()),
          true
        )
        .flatMap(getResult -> getResult.map(Mono::just).orElseGet(Mono::empty));
    });
  }

  /**
   * Reads from all available replicas and the active node and returns the results as a flux.
   *
   * <p>Note that individual errors are ignored, so you can think of this API as a best effort
   * approach which explicitly emphasises availability over consistency.</p>
   *
   * @param id the document id.
   * @return a flux of results from the active and the replica.
   */
  public Flux<GetResult> getFromReplica(final String id) {
    return getFromReplica(id, GetFromReplicaOptions.DEFAULT);
  }

  /**
   * Reads all available or one replica and returns the results as a flux.
   *
   * <p>By default all available replicas and the active node will be asked and returned as
   * an async stream. If configured differently in the options</p>
   *
   * @param id the document id.
   * @param options the custom options.
   * @return a flux of results from the active and the replica depending on the options.
   */
  public Flux<GetResult> getFromReplica(final String id, final GetFromReplicaOptions options) {
    return Flux
      .fromStream(asyncCollection.getFromReplicaRequests(id, options))
      .flatMap(request -> {

        Mono<GetResult> result = Reactor
          .wrap(request, GetAccessor.get(core, id, request), true)
          .flatMap(getResult -> getResult.map(Mono::just).orElseGet(Mono::empty));

        GetFromReplicaOptions.BuiltGetFromReplicaOptions opts = options.build();
        if (opts.replicaMode() == ReplicaMode.ALL) {
          result = result.onErrorResume(t -> {
            coreContext.environment().eventBus().publish(new IndividualReplicaGetFailedEvent(
              request.context()
            ));
            return Mono.empty();
          });
        }

        return result;
      });
  }

  /**
   * Checks if the given document ID exists on the active partition with default options.
   *
   * @param id the document ID
   * @return a {@link Mono} completing once loaded or failed.
   */
  public Mono<ExistsResult> exists(final String id) {
    return exists(id, ExistsOptions.DEFAULT);
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
      ObserveViaCasRequest request = asyncCollection.existsRequest(id, options);
      return Reactor
        .wrap(request, ExistsAccessor.exists(core, id, request), true)
        .flatMap(getResult -> getResult.map(Mono::just).orElseGet(Mono::empty));
    });
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
    return Mono.defer(() -> {
      RemoveOptions.BuiltRemoveOptions opts = options.build();
      RemoveRequest request = asyncCollection.removeRequest(id, options);
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
  public Mono<MutationResult> insert(final String id, Object content, final InsertOptions options) {
    return Mono.defer(() -> {
      InsertOptions.BuiltInsertOptions opts = options.build();
      InsertRequest request = asyncCollection.insertRequest(id, content, options);
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
  public Mono<MutationResult> upsert(final String id, Object content, final UpsertOptions options) {
    return Mono.defer(() -> {
      UpsertRequest request = asyncCollection.upsertRequest(id, content, options);
      UpsertOptions.BuiltUpsertOptions opts = options.build();
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
  public Mono<MutationResult> replace(final String id, Object content, final ReplaceOptions options) {
    return Mono.defer(() -> {
      ReplaceOptions.BuiltReplaceOptions opts = options.build();
      ReplaceRequest request = asyncCollection.replaceRequest(id, content, options);
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
    return touch(id, expiry, TouchOptions.DEFAULT);
  }

  /**
   * Updates the expiry of the document with the given id with custom options.
   *
   * @param id the id of the document to update.
   * @param expiry the new expiry for the document.
   * @param options the custom options.
   * @return a {@link MutationResult} once the operation completes.
   */
  public Mono<MutationResult> touch(final String id, final Duration expiry,
                                    final TouchOptions options) {
    return Mono.defer(() -> {
      TouchOptions.BuiltTouchOptions opts = options.build();
      TouchRequest request = asyncCollection.touchRequest(id, expiry, options);
      return Reactor.wrap(
        request,
        TouchAccessor.touch(core, request, id, opts.persistTo(), opts.replicateTo()),
        true
      );
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
    return unlock(id, cas, UnlockOptions.DEFAULT);
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
      return Reactor.wrap(request, UnlockAccessor.unlock(core, request), true);
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
    return lookupIn(id, specs, LookupInOptions.DEFAULT);
  }

  /**
   * Performs lookups to document fragments with custom options.
   *
   * @param id the outer document ID.
   * @param specs the spec which specifies the type of lookups to perform.
   * @param options custom options to modify the lookup options.
   * @return the {@link LookupInResult} once the lookup has been performed or failed.
   */
  public Mono<LookupInResult> lookupIn(final String id, List<LookupInSpec> specs,
                                       final LookupInOptions options) {
    return Mono.defer(() -> {
      SubdocGetRequest request = asyncCollection.lookupInRequest(id, specs, options);
      return Reactor
        .wrap(request, LookupInAccessor.lookupInAccessor(core, id, request), true)
        .flatMap(getResult -> getResult.map(Mono::just).orElseGet(Mono::empty));
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
    return mutateIn(id, specs, MutateInOptions.DEFAULT);
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
      MutateInOptions.BuiltMutateInOptions opts = options.build();
      SubdocMutateRequest request = asyncCollection.mutateInRequest(id, specs, options);
      return Reactor.wrap(
        request,
        MutateInAccessor.mutateIn(core, request, id, opts.persistTo(), opts.replicateTo()),
        true
      );
    });
  }


}
