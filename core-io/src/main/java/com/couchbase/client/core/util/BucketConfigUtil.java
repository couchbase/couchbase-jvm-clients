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
import com.couchbase.client.core.retry.reactor.Backoff;
import com.couchbase.client.core.retry.reactor.Retry;
import com.couchbase.client.core.retry.reactor.RetryExhaustedException;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * Defines helpful routines for working with bucket configs.
 */
@Stability.Internal
public class BucketConfigUtil {
    private BucketConfigUtil() {}

    private static final Duration retryDelay = Duration.ofMillis(100);

    /**
     * A bucket config can be null while the bucket has not been opened.  This method allows easily for a config to be
     * available.
     */
    public static Mono<BucketConfig> waitForBucketConfig(final Core core,
                                                         final String bucketName,
                                                         final Duration timeout) {
        return Mono.fromCallable(() -> {
            final BucketConfig bucketConfig = core.clusterConfig().bucketConfig(bucketName);
            if (bucketConfig == null) {
                throw new NullPointerException();
            }
            return bucketConfig;
        }).retryWhen(Retry.anyOf(NullPointerException.class)
                .timeout(timeout)
                .backoff(Backoff.fixed(retryDelay))
                .toReactorRetry())
                .onErrorResume(err -> {
                    if (err instanceof RetryExhaustedException) {
                        return Mono.error(new UnambiguousTimeoutException("Timed out while waiting for bucket config", null));
                    } else {
                        return Mono.error(err);
                    }
                });
    }
}
