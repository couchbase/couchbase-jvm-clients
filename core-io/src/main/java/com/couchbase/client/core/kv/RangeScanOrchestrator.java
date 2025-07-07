/*
 * Copyright (c) 2022 Couchbase, Inc.
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

package com.couchbase.client.core.kv;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.Reactor;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.kv.CoreKvParamValidators;
import com.couchbase.client.core.error.AuthenticationFailureException;
import com.couchbase.client.core.error.CollectionNotFoundException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.error.InternalServerFailureException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.error.MutationTokenOutdatedException;
import com.couchbase.client.core.error.RangeScanPartitionFailedException;
import com.couchbase.client.core.error.RequestCanceledException;
import com.couchbase.client.core.error.UnambiguousTimeoutException;
import com.couchbase.client.core.error.context.CancellationErrorContext;
import com.couchbase.client.core.error.context.KeyValueErrorContext;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.CancellationReason;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.kv.RangeScanCancelRequest;
import com.couchbase.client.core.msg.kv.RangeScanContinueRequest;
import com.couchbase.client.core.msg.kv.RangeScanCreateRequest;
import com.couchbase.client.core.topology.BucketCapability;
import com.couchbase.client.core.topology.ClusterTopologyWithBucket;
import com.couchbase.client.core.topology.CouchbaseBucketTopology;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import static com.couchbase.client.core.util.BucketConfigUtil.waitForBucketTopology;
import static com.couchbase.client.core.util.Validators.notNull;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Main entry point from higher level languages to perform KV range scans.
 * <p>
 * This class is meant as a low-level abstraction which is to be consumed from the higher level language bindings
 * and not directly by the user. See the respective documentation for each language binding (java, scala, kotlin)
 * for example usage.
 */
@Stability.Internal
public class RangeScanOrchestrator {
  public static final int RANGE_SCAN_DEFAULT_BATCH_BYTE_LIMIT = 15000;
  public static final int RANGE_SCAN_DEFAULT_BATCH_ITEM_LIMIT = 50;

  private final Core core;
  private final CollectionIdentifier collectionIdentifier;

  /**
   * Creates a new {@link RangeScanOrchestrator} which can be shared across calls.
   *
   * @param core the core to perform ops against.
   * @param collectionIdentifier the pointer to the right collection to use.
   */
  public RangeScanOrchestrator(final Core core, final CollectionIdentifier collectionIdentifier) {
    this.core = notNull(core, "Core");
    this.collectionIdentifier = notNull(collectionIdentifier, "CollectionIdentifier");
  }

  private Mono<CouchbaseBucketTopology> currentBucketTopology(CoreScanOptions options) {
    Duration scanTimeout = options.commonOptions().timeout().orElse(core.environment().timeoutConfig().kvScanTimeout());
    Duration timeout = min(scanTimeout, core.environment().timeoutConfig().connectTimeout()); // default scan timeout is 75 seconds; don't want to wait that long!

    return waitForBucketTopology(core, collectionIdentifier.bucket(), timeout)
      .map(ClusterTopologyWithBucket::bucket)
      .cast(CouchbaseBucketTopology.class)
      .onErrorMap(ClassCastException.class, t -> new FeatureNotAvailableException("Only Couchbase buckets are supported with KV Range Scan", t))

      .filter(bucket -> bucket.hasCapability(BucketCapability.RANGE_SCAN))
      .switchIfEmpty(Mono.error(FeatureNotAvailableException::rangeScan));
  }

  private static <T extends Comparable<T>> T min(T a, T b) {
    return a.compareTo(b) < 0 ? a : b;
  }

  public Flux<CoreRangeScanItem> rangeScan(CoreRangeScan rangeScan, CoreScanOptions options) {
    return Flux.defer(() -> {
      CoreKvParamValidators.validateScanParams(rangeScan, options);

      return currentBucketTopology(options)
        .flatMapMany(topology -> streamForPartitions(
          topology,
          options,
          (partition, start) -> {
            byte[] actualStartTerm = start == null ? rangeScan.from().id().getBytes(UTF_8) : start;
            return RangeScanCreateRequest.forRangeScan(
              actualStartTerm,
              rangeScan,
              options,
              partition,
              core.context(),
              collectionIdentifier,
              options.consistencyMap()
            );
          }));
    });
  }

  public Flux<CoreRangeScanItem> samplingScan(CoreSamplingScan samplingScan, CoreScanOptions options) {
    return Flux.defer(() -> {
      CoreKvParamValidators.validateScanParams(samplingScan, options);

      return currentBucketTopology(options)
        .flatMapMany(topology -> streamForPartitions(
          topology,
          options,
          (partition, ignored) -> RangeScanCreateRequest.forSamplingScan(
            samplingScan,
            options,
            partition,
            core.context(),
            collectionIdentifier,
            options.consistencyMap()
          )
        ))
        .take(samplingScan.limit());
    });
  }

  private Flux<CoreRangeScanItem> streamForPartitions(
    CouchbaseBucketTopology topology,
    CoreScanOptions options,
    BiFunction<Short, byte[], RangeScanCreateRequest> createSupplier
  ) {
    final AtomicLong itemsStreamed = new AtomicLong();

    int numPartitions = topology.numberOfPartitions();

    List<Flux<CoreRangeScanItem>> partitionStreams = new ArrayList<>(numPartitions);
    for (short i = 0; i < numPartitions; i++) {
      partitionStreams.add(streamForPartition(i, createSupplier, options));
    }

    Flux<CoreRangeScanItem> stream = Flux.concat(partitionStreams);

    return stream
      .doOnNext(item -> itemsStreamed.incrementAndGet())
      .timeout(options.commonOptions().timeout().orElse(core.context().environment().timeoutConfig().kvScanTimeout()), Mono.defer(() -> Mono.error(
        new UnambiguousTimeoutException("RangeScan timed out", new CancellationErrorContext(new RangeScanContext(itemsStreamed.get())))
      )));
  }

  private Flux<CoreRangeScanItem> streamForPartition(final short partition,
                                                     final BiFunction<Short, byte[], RangeScanCreateRequest> createSupplier,
                                                     final CoreScanOptions options) {
    final AtomicReference<byte[]> lastStreamed = new AtomicReference<>();
    final AtomicBoolean needToCancel = new AtomicBoolean(false);

    return Flux.defer(() -> {
        RangeScanCreateRequest request = createSupplier.apply(partition, lastStreamed.get());
        core.send(request);
        Flux<CoreRangeScanItem> inner = Reactor
          .wrap(request, request.response(), true)
          .flatMapMany(res -> {
            if (res.status().success()) {
              if (needToCancel.get()) {
                return cancel(res.rangeScanId(), partition, options)
                  .thenMany(Flux.empty());
              }
              return continueScan(partition, res.rangeScanId(), options, needToCancel);
            }

            final KeyValueErrorContext errorContext = KeyValueErrorContext.completedRequest(request, res);
            switch (res.status()) {
              case NOT_FOUND:
                return Flux.empty();
              case INTERNAL_SERVER_ERROR:
                return Flux.error(new InternalServerFailureException(errorContext));
              case VBUUID_NOT_EQUAL:
                return Flux.error(new MutationTokenOutdatedException(errorContext));
              default:
                return Flux.error(new CouchbaseException(res.toString(), errorContext));
            }
          });
        return Reactor.shieldFromCancellation(inner);
      })
      .doOnNext(item -> lastStreamed.set(item.keyBytes()))
      .retryWhen(Retry.from(companion -> companion.map(rs -> {
        if (rs.failure() instanceof RangeScanPartitionFailedException) {
          if (((RangeScanPartitionFailedException) rs.failure()).status() == ResponseStatus.NOT_MY_VBUCKET) {
            return true;
          }
        }
        throw Exceptions.propagate(rs.failure());
      })))
      .doOnCancel(() -> {
        needToCancel.set(true);
      });
  }

  private Flux<CoreRangeScanItem> continueScan(final short partition,
                                               final CoreRangeScanId id,
                                               final CoreScanOptions options,
                                               final AtomicBoolean needToCancel) {

    final AtomicBoolean complete = new AtomicBoolean(false);

    return Flux
      .defer(() -> {
          RangeScanContinueRequest request = new RangeScanContinueRequest(id,
              Sinks.many().unicast().onBackpressureBuffer(), null, options, partition, core.context(), collectionIdentifier);
        core.send(request);
        return Reactor
          .wrap(request, request.response(), true)
          .flatMapMany(res -> {
            if (res.status() == ResponseStatus.SUCCESS || res.status() == ResponseStatus.COMPLETE || res.status() == ResponseStatus.CONTINUE) {
              if (needToCancel.get()) {
                complete.set(true);
                return cancel(id, partition, options)
                  .thenMany(Flux.empty());
              }
              return res.items();
            }

            final KeyValueErrorContext errorContext = KeyValueErrorContext.completedRequest(request, res);
            switch (res.status()) {
              case NOT_FOUND:
                return Flux.error(new CouchbaseException("The range scan internal partition UUID could not be found on the server", errorContext));
              case INVALID_REQUEST:
                return Flux.error(new InvalidArgumentException("The request failed the server-side input validation check.", null, errorContext));
              case NO_ACCESS:
                return Flux.error(new AuthenticationFailureException("The user is no longer authorized to perform this operation", errorContext, null));
              case CANCELED:
                return Flux.error(new RequestCanceledException("The range scan was cancelled.", CancellationReason.OTHER, new CancellationErrorContext(errorContext)));
              case NOT_MY_VBUCKET:
                // The NMVB will not bubble up tho the user, it will be caught at a higher level to perform retry logic.
                return Flux.error(new RangeScanPartitionFailedException("Received \"Not My VBucket\" for the continue response", res.status()));
              case UNKNOWN_COLLECTION:
                return Flux.error(new CollectionNotFoundException(
                  request.collectionIdentifier().collection().orElse(CollectionIdentifier.DEFAULT_COLLECTION),
                  errorContext)
                );
              case SERVER_BUSY:
                return Flux.error(new CouchbaseException("The range scan for this partition is already streaming " +
                  "on another connection - this is a SDK bug please report.", errorContext));
              default:
                return Flux.error(new CouchbaseException(res.toString(), errorContext));
            }
          });
      })
      .map(item -> {
        if (item instanceof LastCoreRangeScanItem) {
          complete.set(true);
        }
        return item;
      })
      .repeat(() -> !complete.get())
      .filter(item -> !(item instanceof LastCoreRangeScanItem));
  }

  /**
   * Cancel the rangescan request.
   */
  private Mono<Void> cancel(
    CoreRangeScanId scanId,
    short partition,
    CoreScanOptions options
  ) {
    return Mono.defer(() -> {
        RangeScanCancelRequest cancelRequest = new RangeScanCancelRequest(
          scanId,
          options,
          partition,
          core.context(),
          collectionIdentifier
        );
        core.send(cancelRequest);
        return Reactor.wrap(cancelRequest, cancelRequest.response(), true);
      })
      .onErrorResume(ignore -> Mono.empty()) // cancellation is best-effort
      .then();
  }

}
