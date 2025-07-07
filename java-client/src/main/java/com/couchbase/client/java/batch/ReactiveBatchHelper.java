/*
 * Copyright (c) 2020 Couchbase, Inc.
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

package com.couchbase.client.java.batch;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.Reactor;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.config.NodeInfo;
import com.couchbase.client.core.env.CoreEnvironment;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.kv.MultiObserveViaCasRequest;
import com.couchbase.client.core.msg.kv.MultiObserveViaCasResponse;
import com.couchbase.client.core.msg.kv.ObserveViaCasResponse;
import com.couchbase.client.core.node.KeyValueLocator;
import com.couchbase.client.core.topology.NodeIdentifier;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.cnc.evnts.BatchHelperExistsCompletedEvent;
import com.couchbase.client.java.kv.GetResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import static com.couchbase.client.core.util.BucketConfigUtil.waitForBucketConfig;

/**
 * This helper class provides methods that make performing batch operations easy and comfortable.
 */
@Stability.Uncommitted
public class ReactiveBatchHelper {

  private static final Predicate<ObserveViaCasResponse.ObserveStatus> PMGET_PREDICATE = s ->
    s == ObserveViaCasResponse.ObserveStatus.FOUND_PERSISTED
      || s == ObserveViaCasResponse.ObserveStatus.FOUND_NOT_PERSISTED;

  /**
   * First checks if the given IDs exist and if so fetches their contents.
   * <p>
   * Please take into consideration when using this API that it only makes sense to use it if of the many ids provided
   * only a small subset comes back. (So let's say you give it 1000 IDs but you only expect 50 to be there or so).
   * Otherwise if all are most of them are there, just use a bulk get with the reactive API directly - you won't see
   * much benefit in this case.
   *
   * @param collection the collection to perform the fetch on.
   * @param ids the document IDs to fetch.
   * @return a Map of the document IDs as the key and the result (if found).
   */
  @Stability.Uncommitted
  public static Mono<Map<String, GetResult>> getIfExists(final Collection collection,
                                                         final java.util.Collection<String> ids) {
    return  Mono.defer(() -> existsBytes(collection, ids)
        .flatMap(e -> {
          String key = new String(e, StandardCharsets.UTF_8);
          return collection.reactive().get(key).map(r -> Tuples.of(key, r));
        })
        .collectMap(Tuple2::getT1, Tuple2::getT2));
  }

  /**
   * Returns a flux of ids for the documents that exist.
   * <p>
   * Note that you usually only want to use this API if you really need to bulk check exists on many documents
   * at once, for all other use cases we recommend trying the regular, supported APIs first
   * (i.e. using {@link com.couchbase.client.java.ReactiveCollection#exists(String)}).
   *
   * @param collection the collection to perform the exists checks on.
   * @param ids the document IDs to check.
   * @return a flux of all the ids that are found.
   */
  @Stability.Uncommitted
  public static Flux<String> exists(final Collection collection, final java.util.Collection<String> ids) {
    return Flux.defer(() -> existsBytes(collection, ids).map(i -> new String(i, StandardCharsets.UTF_8)));
  }

  /**
   * Performs the bulk logic of fetching a config and splitting up the observe requests.
   *
   * @param collection the collection on which the query should be performed.
   * @param ids the list of ids which should be checked.
   * @return a flux of all
   */
  private static Flux<byte[]> existsBytes(final Collection collection, final java.util.Collection<String> ids) {
    final Core core = collection.core();
    final CoreEnvironment env = core.context().environment();

    return waitForBucketConfig(core, collection.bucketName(), env.timeoutConfig().kvTimeout())
      .cast(CouchbaseBucketConfig.class)
      .onErrorMap(ClassCastException.class, t -> new IllegalStateException("Only couchbase (and ephemeral) buckets are supported at this point!"))
      .flatMapMany(bucketConfig -> {
        long start = System.nanoTime();

        Map<NodeIdentifier, Map<byte[], Short>> nodeEntries = new HashMap<>(bucketConfig.nodes().size());
        for (NodeInfo node : bucketConfig.nodes()) {
          nodeEntries.put(node.id(), new HashMap<>(ids.size() / bucketConfig.nodes().size()));
        }

        CollectionIdentifier ci = new CollectionIdentifier(
          collection.bucketName(),
          Optional.of(collection.scopeName()),
          Optional.of(collection.name())
        );

        for (String id : ids) {
          byte[] encodedId = id.getBytes(StandardCharsets.UTF_8);
          int partitionId = KeyValueLocator.partitionForKey(encodedId, bucketConfig.numberOfPartitions());
          int nodeId = bucketConfig.nodeIndexForActive(partitionId, false);
          NodeInfo nodeInfo = bucketConfig.nodeAtIndex(nodeId);
          nodeEntries.get(nodeInfo.id()).put(encodedId, (short) partitionId);
        }

        List<Mono<MultiObserveViaCasResponse>> responses = new ArrayList<>(nodeEntries.size());
        List<MultiObserveViaCasRequest> requests = new ArrayList<>(nodeEntries.size());

        for (Map.Entry<NodeIdentifier, Map<byte[], Short>> node : nodeEntries.entrySet()) {
          if (node.getValue().isEmpty()) {
            // We need to make sure to only send requests to the nodes which 1) have the kv
            // service enabled and 2) have keys that we need to fetch
            continue;
          }

          MultiObserveViaCasRequest request = new MultiObserveViaCasRequest(
            env.timeoutConfig().kvTimeout(),
            core.context(),
            env.retryStrategy(),
            ci,
            node.getKey(),
            node.getValue(),
            PMGET_PREDICATE
          );
          core.send(request);
          requests.add(request);
          responses.add(Reactor.wrap(request, request.response(), true));
        }

        return env.publishOnUserScheduler(Flux
          .merge(responses)
          .flatMap(response -> Flux.fromIterable(response.observed().keySet()))
          .onErrorMap(throwable -> {
            BatchErrorContext ctx = new BatchErrorContext(Collections.unmodifiableList(requests));
            return new BatchHelperFailureException("Failed to perform BatchHelper bulk operation", throwable, ctx);
          })
          .doOnComplete(() -> core.context().environment().eventBus().publish(new BatchHelperExistsCompletedEvent(
            Duration.ofNanos(System.nanoTime() - start),
            new BatchErrorContext(Collections.unmodifiableList(requests))
          ))));
      });
  }

}
