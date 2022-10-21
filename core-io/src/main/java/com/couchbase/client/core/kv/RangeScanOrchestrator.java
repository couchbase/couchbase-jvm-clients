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
import com.couchbase.client.core.CoreContext;
import com.couchbase.client.core.Reactor;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.cnc.RequestSpan;
import com.couchbase.client.core.cnc.TracingIdentifiers;
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
import com.couchbase.client.core.msg.kv.RangeScanContinueRequest;
import com.couchbase.client.core.msg.kv.RangeScanCreateRequest;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import static com.couchbase.client.core.util.Validators.notNull;

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
   * @param startTerm the start term used for the range scan.
   * @param startExclusive if the start term is exclusive or inclusive.
   * @param endTerm the end term used for the range scan.
   * @param endExclusive if the end term is exclusive or inclusive.
   * @param timeout the timeout for the full operation.
   * @param continueItemLimit the number of items to load (max) per continue operation.
   * @param continueByteLimit the number of bytes to load (max) per continue operation.
   * @param keysOnly if only keys should be loaded.
   * @param sort the sorting to apply (could be none).
   * @param parent the optional span parent to use.
   * @return a {@link Flux} of returned items, or a failed flux during errors.
   */
  public Flux<CoreRangeScanItem> rangeScan(byte[] startTerm, boolean startExclusive, byte[] endTerm,
                                           boolean endExclusive, Duration timeout, int continueItemLimit,
                                           int continueByteLimit, boolean keysOnly, CoreRangeScanSort sort,
                                           Optional<RequestSpan> parent, Map<Short, MutationToken> consistencyTokens) {
    return Flux.defer(() -> {
      if (currentBucketConfig == null) {
        // We might not have a config yet if bootstrap is still in progress, wait 100ms
        // and then try again. In a steady state this should not happen.
        return Mono
          .delay(Duration.ofMillis(100), core.context().environment().scheduler())
          .flatMapMany(ign -> rangeScan(startTerm, startExclusive, endTerm, endExclusive, timeout,
            continueItemLimit, continueByteLimit, keysOnly, sort, parent, consistencyTokens));
      } else if (!(currentBucketConfig instanceof CouchbaseBucketConfig)) {
        return Flux.error(new IllegalStateException("Only Couchbase buckets are supported with KV Range Scan"));
      }

      return streamForPartitions((partition, start) -> {
        CoreContext ctx = core.context();
        RequestSpan span = ctx
          .environment()
          .requestTracer()
          .requestSpan(TracingIdentifiers.SPAN_REQUEST_KV_RANGE_SCAN_CREATE, parent.orElse(null));

        Optional<MutationToken> mutationToken = Optional.ofNullable(consistencyTokens.get(partition));

        byte[] actualStartTerm = start == null ? startTerm : start;
        return RangeScanCreateRequest.forRangeScan(actualStartTerm, startExclusive, endTerm, endExclusive, keysOnly, timeout,
          core.context(), core.context().environment().retryStrategy(), collectionIdentifier, span, partition,
          mutationToken);
      }, sort, timeout, continueItemLimit, continueByteLimit, parent, keysOnly);
    });
  }

  /**
   * Performs a sampling scan (reactive).
   *
   * @param limit the number of items to load for the sampling scan.
   * @param seed the seed number to be used.
   * @param timeout the timeout for the full operation.
   * @param continueItemLimit the number of items to load (max) per continue operation.
   * @param continueByteLimit the number of bytes to load (max) per continue operation.
   * @param keysOnly if only keys should be loaded.
   * @param sort the sorting to apply (could be none).
   * @param parent the optional span parent to use.
   * @return a {@link Flux} of returned items, or a failed flux during errors.
   */
  public Flux<CoreRangeScanItem> samplingScan(long limit, Optional<Long> seed, Duration timeout, int continueItemLimit,
                                              int continueByteLimit, boolean keysOnly, CoreRangeScanSort sort,
                                              Optional<RequestSpan> parent, Map<Short, MutationToken> consistencyTokens) {
    return Flux
      .defer(() -> {
        if (currentBucketConfig == null) {
          // We might not have a config yet if bootstrap is still in progress, wait 100ms
          // and then try again. In a steady state this should not happen.
          return Mono
            .delay(Duration.ofMillis(100), core.context().environment().scheduler())
            .flatMapMany(ign -> samplingScan(limit, seed, timeout, continueItemLimit, continueByteLimit,
              keysOnly, sort, parent, consistencyTokens));
        } else if (!(currentBucketConfig instanceof CouchbaseBucketConfig)) {
          return Flux.error(new IllegalStateException("Only Couchbase buckets are supported with KV Range Scan"));
        }

        return streamForPartitions((partition, ignored) -> {
          CoreContext ctx = core.context();
          RequestSpan span = ctx
            .environment()
            .requestTracer()
            .requestSpan(TracingIdentifiers.SPAN_REQUEST_KV_RANGE_SCAN_CREATE, parent.orElse(null));

          Optional<MutationToken> mutationToken = Optional.ofNullable(consistencyTokens.get(partition));

          return RangeScanCreateRequest.forSamplingScan(limit, seed, keysOnly, timeout, ctx,
            ctx.environment().retryStrategy(), collectionIdentifier, span, partition, mutationToken);
        }, sort, timeout, continueItemLimit, continueByteLimit, parent, keysOnly);

      })
      .take(limit);
  }

  @SuppressWarnings("unchecked")
  private Flux<CoreRangeScanItem> streamForPartitions(final BiFunction<Short, byte[], RangeScanCreateRequest> createSupplier,
                                                      final CoreRangeScanSort sort,
                                                      final Duration timeout, final int itemLimit,
                                                      final int byteLimit, Optional<RequestSpan> parent,
                                                      final boolean keysOnly) {

    if (!capabilityEnabled) {
      return Flux.error(FeatureNotAvailableException.rangeScan());
    }

    final AtomicLong itemsStreamed = new AtomicLong();

    int numPartitions = ((CouchbaseBucketConfig) currentBucketConfig).numberOfPartitions();

    List<Flux<CoreRangeScanItem>> partitionStreams = new ArrayList<>(numPartitions);
    for (short i = 0; i < numPartitions; i++) {
      partitionStreams.add(streamForPartition(i, createSupplier, timeout, itemLimit, byteLimit, parent, keysOnly));
    }

    Flux<CoreRangeScanItem> stream;

    if (CoreRangeScanSort.ASCENDING == sort) {
      stream = Flux.mergeComparing(
        Comparator.comparing(CoreRangeScanItem::key),
        partitionStreams.toArray(new Flux[0])
      );
    } else {
      stream = Flux.merge(partitionStreams);
    }

    return stream
      .doOnNext(item -> itemsStreamed.incrementAndGet())
      .timeout(timeout, Mono.defer(() -> Mono.error(
        new UnambiguousTimeoutException("RangeScan timed out", new CancellationErrorContext(new RangeScanContext(itemsStreamed.get())))
      )));
  }

  private Flux<CoreRangeScanItem> streamForPartition(final short partition,
                                                     final BiFunction<Short, byte[], RangeScanCreateRequest> createSupplier,
                                                     final Duration timeout, final int itemLimit, final int byteLimit,
                                                     Optional<RequestSpan> parent, final boolean keysOnly) {
    final AtomicReference<byte[]> lastStreamed = new AtomicReference<>();

    return Flux
      .defer(() -> {
        RangeScanCreateRequest request = createSupplier.apply(partition, lastStreamed.get());
        core.send(request);
        return Reactor
          .wrap(request, request.response(), true)
          .flatMapMany(res -> {
            if (res.status().success()) {
              return continueScan(timeout, partition, res.rangeScanId(), itemLimit, byteLimit, parent, keysOnly);
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
        })
      .doOnNext(item -> lastStreamed.set(item.keyBytes()))
      .retryWhen(Retry.from(companion -> companion.map(rs -> {
        if (rs.failure() instanceof RangeScanPartitionFailedException) {
          if (((RangeScanPartitionFailedException) rs.failure()).status() == ResponseStatus.NOT_MY_VBUCKET) {
            return true;
          }
        }
        return Exceptions.propagate(rs.failure());
      })));
  }

  private Flux<CoreRangeScanItem> continueScan(final Duration timeout, final short partition, final CoreRangeScanId id,
                                               final int itemLimit, final int byteLimit, Optional<RequestSpan> parent,
                                               final boolean keysOnly) {
    final AtomicBoolean complete = new AtomicBoolean(false);

    return Flux
      .defer(() -> {
        CoreContext ctx = core.context();

        RequestSpan span = ctx
          .environment()
          .requestTracer()
          .requestSpan(TracingIdentifiers.SPAN_REQUEST_KV_RANGE_SCAN_CONTINUE, parent.orElse(null));

        RangeScanContinueRequest request = new RangeScanContinueRequest(id, itemLimit, byteLimit, timeout, ctx,
          ctx.environment().retryStrategy(), null, collectionIdentifier, span,
          Sinks.many().unicast().onBackpressureBuffer(), partition, keysOnly
        );
        core.send(request);
        return Reactor
          .wrap(request, request.response(), true)
          .flatMapMany(res -> {
            if (res.status() == ResponseStatus.SUCCESS || res.status() == ResponseStatus.COMPLETE) {
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

}
