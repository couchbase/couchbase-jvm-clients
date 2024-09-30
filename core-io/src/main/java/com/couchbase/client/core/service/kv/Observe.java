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

package com.couchbase.client.core.service.kv;

import com.couchbase.client.core.Reactor;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.error.ReplicaNotConfiguredException;
import com.couchbase.client.core.msg.kv.MutationToken;
import com.couchbase.client.core.msg.kv.ObserveViaSeqnoRequest;
import com.couchbase.client.core.retry.RetryStrategy;
import com.couchbase.client.core.retry.reactor.Repeat;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Implements traditional observe-based durability requirements.
 *
 * @since 2.0.0
 */
public class Observe {

  public static Mono<Void> poll(final ObserveContext ctx) {
    if (ctx.persistTo() == ObservePersistTo.NONE && ctx.replicateTo() == ObserveReplicateTo.NONE) {
      return Mono.empty();
    }

    if (!ctx.environment().ioConfig().mutationTokensEnabled() || !ctx.mutationToken().isPresent()) {
      return Mono.error(
        new FeatureNotAvailableException("To use PersistTo and/or ReplicateTo, mutation tokens must " +
          "be enabled on the IO configuration")
      );
    }

    final RequestSpan parentSpan = ctx
      .coreResources()
      .requestTracer()
      .requestSpan("observe", ctx.parentSpan());

    Flux<ObserveItem> observed = Flux.defer(() -> {
      BucketConfig config = ctx.core().clusterConfig().bucketConfig(ctx.collectionIdentifier().bucket());
      return Flux.just(validateReplicas(config, ctx.persistTo(), ctx.replicateTo()));
    })
    .flatMap(replicas -> viaMutationToken(replicas, ctx, parentSpan));
    return maybeRetry(observed, ctx).timeout(ctx.timeout(), ctx.environment().scheduler()).doFinally(t -> parentSpan.end());
  }

  private static Flux<ObserveItem> viaMutationToken(final int bucketReplicas, final ObserveContext ctx,
                                                    final RequestSpan parent) {
    if (!ctx.mutationToken().isPresent()) {
      throw new IllegalStateException("MutationToken is not present, this is a bug!");
    }

    Duration timeout = ctx.timeout();
    RetryStrategy retryStrategy = ctx.retryStrategy();
    MutationToken mutationToken = ctx.mutationToken().get();
    String id = ctx.key();

    List<ObserveViaSeqnoRequest> requests = new ArrayList<>();
    if (ctx.persistTo() != ObservePersistTo.NONE) {
      final RequestSpan span = ctx.coreResources().requestTracer()
        .requestSpan(TracingIdentifiers.SPAN_REQUEST_KV_OBSERVE, parent);
      requests.add(new ObserveViaSeqnoRequest(timeout, ctx, ctx.collectionIdentifier(), retryStrategy, 0, true,
        mutationToken.partitionUUID(), id, span));
    }

    if (ctx.persistTo().touchesReplica() || ctx.replicateTo().touchesReplica()) {
      for (short i = 1; i <= bucketReplicas; i++) {
        final RequestSpan span = ctx.coreResources().requestTracer()
          .requestSpan(TracingIdentifiers.SPAN_REQUEST_KV_OBSERVE, parent);
        requests.add(new ObserveViaSeqnoRequest(timeout, ctx, ctx.collectionIdentifier(), retryStrategy, i, false,
          mutationToken.partitionUUID(), id, span));
      }
    }

    return Flux.fromIterable(requests)
      .flatMap(request -> {
        ctx.core().send(request);
        return Reactor
          .wrap(request, request.response(), true)
          .doOnNext(ignored -> request.context().logicallyComplete())
          .doOnError(err -> request.context().logicallyComplete(err))
          .doOnCancel(() -> request.context().logicallyComplete())
          .onErrorResume(t-> Mono.empty());
      })
      .map(response -> ObserveItem.fromMutationToken(mutationToken, response));
  }


  private static Mono<Void> maybeRetry(Flux<ObserveItem> observedItems, final ObserveContext ctx) {
    return observedItems
      .scan(ObserveItem.empty(), ObserveItem::add)
      .repeatWhen(Repeat.times(Long.MAX_VALUE).exponentialBackoff(Duration.ofNanos(10000), Duration.ofMillis(100)))
      .skipWhile(status -> !status.check(ctx.persistTo(), ctx.replicateTo()))
      .take(1)
      .then();
  }

  private static int validateReplicas(final BucketConfig bucketConfig, final ObservePersistTo persistTo,
                                      final ObserveReplicateTo replicateTo) {
    if (!(bucketConfig instanceof CouchbaseBucketConfig)) {
      throw new FeatureNotAvailableException("Only couchbase buckets support PersistTo and/or ReplicateTo");
    }
    CouchbaseBucketConfig cbc = (CouchbaseBucketConfig) bucketConfig;
    int numReplicas = cbc.numberOfReplicas();

    if (cbc.ephemeral() && persistTo.value() != 0) {
      throw new FeatureNotAvailableException("Ephemeral Buckets do not support " +
        "PersistTo");
    }
    if (replicateTo.touchesReplica() && replicateTo.value() > numReplicas) {
      throw new ReplicaNotConfiguredException("Not enough replicas configured on " +
        "the bucket");
    }
    if (persistTo.touchesReplica() && persistTo.value() - 1 > numReplicas) {
      throw new ReplicaNotConfiguredException("Not enough replicas configured on " +
        "the bucket");
    }

    return numReplicas;
  }

  /**
   * Defines the possible disk persistence constraints to observe.
   *
   * @author Michael Nitschinger
   * @since 1.0.1
   */
  public enum ObservePersistTo {
    /**
     * Observe disk persistence to the active node of the document only.
     */
    ACTIVE((short) -1),

    /**
     * Do not observe any disk persistence constraint.
     */
    NONE((short) 0),

    /**
     * Observe disk persistence of one node (active or replica).
     */
    ONE((short) 1),

    /**
     * Observe disk persistence of two nodes (active or replica).
     */
    TWO((short) 2),

    /**
     * Observe disk persistence of three nodes (active or replica).
     */
    THREE((short) 3),

    /**
     * Observe disk persistence of four nodes (one active and three replicas).
     */
    FOUR((short) 4);

    /**
     * Contains the internal value to map onto.
     */
    private final short value;

    /**
     * Internal constructor for the enum.
     *
     * @param value the value of the persistence constraint.
     */
    ObservePersistTo(short value) {
      this.value = value;
    }

    /**
     * Returns the actual internal persistence representation for the enum.
     *
     * @return the internal persistence representation.
     */
    public short value() {
      return value;
    }

    /**
     * Identifies if this enum property will touch a replica or just the active.
     *
     * @return true if it includes a replica, false if not.
     */
    public boolean touchesReplica() {
      return value > 0;
    }
  }

  /**
   * Defines the possible replication constraints to observe.
   *
   * @author Michael Nitschinger
   * @since 1.0.1
   */
  public enum ObserveReplicateTo {

    /**
     * Do not observe any replication constraint.
     */
    NONE((short) 0),

    /**
     * Observe replication to one replica.
     */
    ONE((short) 1),

    /**
     * Observe replication to two replicas.
     */
    TWO((short) 2),

    /**
     * Observe replication to three replicas.
     */
    THREE((short) 3);

    /**
     * Contains the internal value to map onto.
     */
    private final short value;

    /**
     * Internal constructor for the enum.
     *
     * @param value the value of the replication constraint.
     */
    ObserveReplicateTo(short value) {
      this.value = value;
    }

    /**
     * Returns the actual internal replication representation for the enum.
     *
     * @return the internal replication representation.
     */
    public short value() {
      return value;
    }

    /**
     * Identifies if this enum property will touch a replica or just the active.
     *
     * @return true if it includes a replica, false if not.
     */
    public boolean touchesReplica() {
      return value > 0;
    }
  }


}
