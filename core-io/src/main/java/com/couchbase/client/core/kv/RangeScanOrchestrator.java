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
import com.couchbase.client.core.config.BucketCapabilities;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.config.CouchbaseBucketConfig;
import com.couchbase.client.core.error.AuthenticationFailureException;
import com.couchbase.client.core.error.CollectionNotFoundException;
import com.couchbase.client.core.error.CouchbaseException;
import com.couchbase.client.core.error.FeatureNotAvailableException;
import com.couchbase.client.core.error.InternalServerFailureException;
import com.couchbase.client.core.error.InvalidArgumentException;
import com.couchbase.client.core.error.MutationTokenOutdatedException;
import com.couchbase.client.core.error.RangeScanCanceledException;
import com.couchbase.client.core.error.RangeScanIdFailureException;
import com.couchbase.client.core.error.RangeScanPartitionFailedException;
import com.couchbase.client.core.error.UnambiguousTimeoutException;
import com.couchbase.client.core.error.context.CancellationErrorContext;
import com.couchbase.client.core.error.context.KeyValueErrorContext;
import com.couchbase.client.core.io.CollectionIdentifier;
import com.couchbase.client.core.msg.ResponseStatus;
import com.couchbase.client.core.msg.kv.MutationToken;
import com.couchbase.client.core.msg.kv.RangeScanCancelRequest;
import com.couchbase.client.core.msg.kv.RangeScanContinueRequest;
import com.couchbase.client.core.msg.kv.RangeScanCreateRequest;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

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

  /**
   * Holds the reference to core.
   */
  private final Core core;

  /**
   * Holds the pointer to the keyspace that should be used.
   */
  private final CollectionIdentifier collectionIdentifier;

  /**
   * Stores the current configuration if already set.
   */
  private volatile BucketConfig currentBucketConfig;

  /**
   * Checks if run against older clusters the feature might not be supported.
   */
  private volatile boolean capabilityEnabled = false;

  /**
   * Creates a new {@link RangeScanOrchestrator} which can be shared across calls.
   *
   * @param core the core to perform ops against.
   * @param collectionIdentifier the pointer to the right collection to use.
   */
  public RangeScanOrchestrator(final Core core, final CollectionIdentifier collectionIdentifier) {
    this.core = notNull(core, "Core");
    this.collectionIdentifier = notNull(collectionIdentifier, "CollectionIdentifier");

    core.configurationProvider().configs().subscribe(cc -> {
      BucketConfig bucketConfig = cc.bucketConfig(collectionIdentifier.bucket());
      if (bucketConfig != null) {
        currentBucketConfig = bucketConfig;
        capabilityEnabled = bucketConfig.bucketCapabilities().contains(BucketCapabilities.RANGE_SCAN);
      }
    });
  }

  /**
   * Performs a range scan between a start and an end term (reactive).
   *
   * @param rangeScan
   * @param options
   * @return a {@link Flux} of returned items, or a failed flux during errors.
   */

    public Flux<CoreRangeScanItem> rangeScan(CoreRangeScan rangeScan, CoreScanOptions options) {
    return Flux.defer(() -> {

      if (currentBucketConfig == null) {
        // We might not have a config yet if bootstrap is still in progress, wait 100ms
        // and then try again. In a steady state this should not happen.
        return Mono
          .delay(Duration.ofMillis(100), core.context().environment().scheduler())
          .flatMapMany(ign -> rangeScan(rangeScan, options));
      } else if (!(currentBucketConfig instanceof CouchbaseBucketConfig)) {
        return Flux.error(new IllegalStateException("Only Couchbase buckets are supported with KV Range Scan"));
      }
      Map<Short, MutationToken> consistencyMap=options.consistencyMap();
      return streamForPartitions((partition, start) -> {
        byte[] actualStartTerm = start == null ? rangeScan.from().id().getBytes(UTF_8) : start;
        return RangeScanCreateRequest.forRangeScan(actualStartTerm, rangeScan, options, partition, core.context(),
            collectionIdentifier, consistencyMap);
      }, options);
    });
  }

  /**
   * Performs a sampling scan (reactive).
   *
   * @param samplingScan
   * @param options
   * @return a {@link Flux} of returned items, or a failed flux during errors.
   */
  public Flux<CoreRangeScanItem> samplingScan(CoreSamplingScan samplingScan, CoreScanOptions options) {
    return Flux
      .defer(() -> {
        if (currentBucketConfig == null) {
          // We might not have a config yet if bootstrap is still in progress, wait 100ms
          // and then try again. In a steady state this should not happen.
          return Mono
            .delay(Duration.ofMillis(100), core.context().environment().scheduler())
            .flatMapMany(ign -> samplingScan(samplingScan, options));
        } else if (!(currentBucketConfig instanceof CouchbaseBucketConfig)) {
          return Flux.error(new IllegalStateException("Only Couchbase buckets are supported with KV Range Scan"));
        }
        Map<Short, MutationToken> consistencyMap=options.consistencyMap();
        return streamForPartitions((partition, ignored) -> RangeScanCreateRequest.forSamplingScan(samplingScan,
              options, partition, core.context(), collectionIdentifier, consistencyMap), options);
        }).take(samplingScan.limit());
  }

  @SuppressWarnings("unchecked")
  private Flux<CoreRangeScanItem> streamForPartitions(final BiFunction<Short, byte[], RangeScanCreateRequest> createSupplier,
                                                      final CoreScanOptions options) {

    if (!capabilityEnabled) {
      return Flux.error(FeatureNotAvailableException.rangeScan());
    }

    final AtomicLong itemsStreamed = new AtomicLong();

    int numPartitions = ((CouchbaseBucketConfig) currentBucketConfig).numberOfPartitions();

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
                return Flux.error(new RangeScanIdFailureException(errorContext));
              case INVALID_REQUEST:
                return Flux.error(new InvalidArgumentException("The request failed the server-side input validation check.", null, errorContext));
              case NO_ACCESS:
                return Flux.error(new AuthenticationFailureException("The user is no longer authorized to perform this operation", errorContext, null));
              case CANCELED:
                return Flux.error(new RangeScanCanceledException(errorContext));
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
