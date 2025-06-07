/*
 * Copyright (c) 2019 Couchbase, Inc.
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
package com.couchbase.client.core.util;

import com.couchbase.client.core.Core;
import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.config.BucketConfig;
import com.couchbase.client.core.error.UnambiguousTimeoutException;
import com.couchbase.client.core.topology.ClusterTopologyWithBucket;
import org.jspecify.annotations.Nullable;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.function.Supplier;

import static com.couchbase.client.core.logging.RedactableArgument.redactMeta;

@Stability.Internal
public class BucketConfigUtil {
  private BucketConfigUtil() {
  }

  private static final Duration retryDelay = Duration.ofMillis(100);

  /**
   * @deprecated In favor of {@link #waitForBucketTopology}
   */
  @Deprecated
  public static Mono<BucketConfig> waitForBucketConfig(
    Core core,
    String bucketName,
    Duration timeout
  ) {
    return waitForBucket(bucketName, timeout, () -> core.clusterConfig().bucketConfig(bucketName));
  }

  public static Mono<ClusterTopologyWithBucket> waitForBucketTopology(
    Core core,
    String bucketName,
    Duration timeout
  ) {
    return waitForBucket(bucketName, timeout, () -> core.clusterConfig().bucketTopology(bucketName));
  }

  private static <T> Mono<T> waitForBucket(
    String bucketName,
    Duration timeout,
    Supplier<@Nullable T> topologySupplier
  ) {
    return Mono.fromSupplier(topologySupplier)
      .repeatWhenEmpty(attempts -> attempts.delayElements(retryDelay))
      .timeout(timeout)
      .onErrorMap(
        java.util.concurrent.TimeoutException.class,
        t -> new UnambiguousTimeoutException(
          "Topology for bucket '" + redactMeta(bucketName) + "' was not available within " + timeout + " -- Does this bucket exist? Does the user have permission to access it? Consider calling bucket.waitUntilReady() first.",
          null
        )
      );
  }
}
