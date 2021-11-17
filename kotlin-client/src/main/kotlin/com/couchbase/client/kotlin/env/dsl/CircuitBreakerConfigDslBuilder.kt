/*
 * Copyright 2021 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.kotlin.env.dsl

import com.couchbase.client.core.endpoint.CircuitBreaker
import com.couchbase.client.core.endpoint.CircuitBreakerConfig
import com.couchbase.client.core.endpoint.CircuitBreakerConfig.DEFAULT_COMPLETION_CALLBACK
import com.couchbase.client.core.endpoint.CircuitBreakerConfig.DEFAULT_ENABLED
import com.couchbase.client.core.endpoint.CircuitBreakerConfig.DEFAULT_ERROR_THRESHOLD_PERCENTAGE
import com.couchbase.client.core.endpoint.CircuitBreakerConfig.DEFAULT_ROLLING_WINDOW
import com.couchbase.client.core.endpoint.CircuitBreakerConfig.DEFAULT_SLEEP_WINDOW
import com.couchbase.client.core.endpoint.CircuitBreakerConfig.DEFAULT_VOLUME_THRESHOLD
import kotlin.properties.Delegates.observable
import kotlin.time.Duration
import kotlin.time.toJavaDuration
import kotlin.time.toKotlinDuration

/**
 * DSL counterpart to [CircuitBreakerConfig.Builder].
 */
@ClusterEnvironmentDslMarker
public class CircuitBreakerConfigDslBuilder(private val wrapped: CircuitBreakerConfig.Builder) {
    /**
     * @see CircuitBreakerConfig.Builder.enabled
     */
    public var enabled: Boolean
            by observable(DEFAULT_ENABLED) { _, _, it -> wrapped.enabled(it) }

    /**
     * @see CircuitBreakerConfig.Builder.volumeThreshold
     */
    public var volumeThreshold: Int
            by observable(DEFAULT_VOLUME_THRESHOLD) { _, _, it -> wrapped.volumeThreshold(it) }

    /**
     * @see CircuitBreakerConfig.Builder.errorThresholdPercentage
     */
    public var errorThresholdPercentage: Int
            by observable(DEFAULT_ERROR_THRESHOLD_PERCENTAGE) { _, _, it -> wrapped.errorThresholdPercentage(it) }

    /**
     * @see CircuitBreakerConfig.Builder.sleepWindow
     */
    public var sleepWindow: Duration
            by observable(DEFAULT_SLEEP_WINDOW.toKotlinDuration()) { _, _, it -> wrapped.sleepWindow(it.toJavaDuration()) }

    /**
     * @see CircuitBreakerConfig.Builder.rollingWindow
     */
    public var rollingWindow: Duration
            by observable(DEFAULT_ROLLING_WINDOW.toKotlinDuration()) { _, _, it -> wrapped.rollingWindow(it.toJavaDuration()) }

    /**
     * @see CircuitBreakerConfig.Builder.completionCallback
     */
    public var completionCallback: CircuitBreaker.CompletionCallback
            by observable(DEFAULT_COMPLETION_CALLBACK) { _, _, it -> wrapped.completionCallback(it) }
}
