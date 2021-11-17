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

import com.couchbase.client.core.env.ThresholdLoggingTracerConfig
import com.couchbase.client.core.env.ThresholdLoggingTracerConfig.Defaults.DEFAULT_ANALYTICS_THRESHOLD
import com.couchbase.client.core.env.ThresholdLoggingTracerConfig.Defaults.DEFAULT_EMIT_INTERVAL
import com.couchbase.client.core.env.ThresholdLoggingTracerConfig.Defaults.DEFAULT_KV_THRESHOLD
import com.couchbase.client.core.env.ThresholdLoggingTracerConfig.Defaults.DEFAULT_QUERY_THRESHOLD
import com.couchbase.client.core.env.ThresholdLoggingTracerConfig.Defaults.DEFAULT_QUEUE_LENGTH
import com.couchbase.client.core.env.ThresholdLoggingTracerConfig.Defaults.DEFAULT_SAMPLE_SIZE
import com.couchbase.client.core.env.ThresholdLoggingTracerConfig.Defaults.DEFAULT_SEARCH_THRESHOLD
import com.couchbase.client.core.env.ThresholdLoggingTracerConfig.Defaults.DEFAULT_VIEW_THRESHOLD
import kotlin.properties.Delegates.observable
import kotlin.time.Duration
import kotlin.time.toJavaDuration
import kotlin.time.toKotlinDuration

/**
 * DSL counterpart to [ThresholdLoggingTracerConfig.Builder].
 */
@ClusterEnvironmentDslMarker
public class ThresholdLoggingTracerConfigDslBuilder(private val wrapped: ThresholdLoggingTracerConfig.Builder) {

    /**
     * @see ThresholdLoggingTracerConfig.Builder.emitInterval
     */
    public var emitInterval: Duration
            by observable(DEFAULT_EMIT_INTERVAL.toKotlinDuration()) { _, _, it -> wrapped.emitInterval(it.toJavaDuration()) }

    /**
     * @see ThresholdLoggingTracerConfig.Builder.queueLength
     */
    public var queueLength: Int
            by observable(DEFAULT_QUEUE_LENGTH) { _, _, it -> wrapped.queueLength(it) }

    /**
     * @see ThresholdLoggingTracerConfig.Builder.sampleSize
     */
    public var sampleSize: Int
            by observable(DEFAULT_SAMPLE_SIZE) { _, _, it -> wrapped.sampleSize(it) }

    /**
     * @see ThresholdLoggingTracerConfig.Builder.kvThreshold
     */
    public var kvThreshold: Duration
            by observable(DEFAULT_KV_THRESHOLD.toKotlinDuration()) { _, _, it -> wrapped.kvThreshold(it.toJavaDuration()) }

    /**
     * @see ThresholdLoggingTracerConfig.Builder.queryThreshold
     */
    public var queryThreshold: Duration
            by observable(DEFAULT_QUERY_THRESHOLD.toKotlinDuration()) { _, _, it -> wrapped.queryThreshold(it.toJavaDuration()) }

    /**
     * @see ThresholdLoggingTracerConfig.Builder.viewThreshold
     */
    public var viewThreshold: Duration
            by observable(DEFAULT_VIEW_THRESHOLD.toKotlinDuration()) { _, _, it -> wrapped.viewThreshold(it.toJavaDuration()) }

    /**
     * @see ThresholdLoggingTracerConfig.Builder.searchThreshold
     */
    public var searchThreshold: Duration
            by observable(DEFAULT_SEARCH_THRESHOLD.toKotlinDuration()) { _, _, it -> wrapped.searchThreshold(it.toJavaDuration()) }

    /**
     * @see ThresholdLoggingTracerConfig.Builder.analyticsThreshold
     */
    public var analyticsThreshold: Duration
            by observable(DEFAULT_ANALYTICS_THRESHOLD.toKotlinDuration()) { _, _, it -> wrapped.analyticsThreshold(it.toJavaDuration()) }
}
