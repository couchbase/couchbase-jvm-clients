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

import com.couchbase.client.core.env.ThresholdRequestTracerConfig
import com.couchbase.client.core.env.ThresholdRequestTracerConfig.Defaults.DEFAULT_ANALYTICS_THRESHOLD
import com.couchbase.client.core.env.ThresholdRequestTracerConfig.Defaults.DEFAULT_EMIT_INTERVAL
import com.couchbase.client.core.env.ThresholdRequestTracerConfig.Defaults.DEFAULT_KV_THRESHOLD
import com.couchbase.client.core.env.ThresholdRequestTracerConfig.Defaults.DEFAULT_QUERY_THRESHOLD
import com.couchbase.client.core.env.ThresholdRequestTracerConfig.Defaults.DEFAULT_QUEUE_LENGTH
import com.couchbase.client.core.env.ThresholdRequestTracerConfig.Defaults.DEFAULT_SAMPLE_SIZE
import com.couchbase.client.core.env.ThresholdRequestTracerConfig.Defaults.DEFAULT_SEARCH_THRESHOLD
import com.couchbase.client.core.env.ThresholdRequestTracerConfig.Defaults.DEFAULT_VIEW_THRESHOLD
import java.time.Duration
import kotlin.properties.Delegates.observable

/**
 * DSL counterpart to [ThresholdRequestTracerConfig.Builder].
 */
@ClusterEnvironmentDslMarker
public class ThresholdRequestTracerConfigDslBuilder(private val wrapped: ThresholdRequestTracerConfig.Builder) {

    /**
     * @see ThresholdRequestTracerConfig.Builder.emitInterval
     */
    public var emitInterval: Duration
            by observable(DEFAULT_EMIT_INTERVAL) { _, _, it -> wrapped.emitInterval(it) }

    /**
     * @see ThresholdRequestTracerConfig.Builder.queueLength
     */
    public var queueLength: Int
            by observable(DEFAULT_QUEUE_LENGTH) { _, _, it -> wrapped.queueLength(it) }

    /**
     * @see ThresholdRequestTracerConfig.Builder.sampleSize
     */
    public var sampleSize: Int
            by observable(DEFAULT_SAMPLE_SIZE) { _, _, it -> wrapped.sampleSize(it) }

    /**
     * @see ThresholdRequestTracerConfig.Builder.kvThreshold
     */
    public var kvThreshold: Duration
            by observable(DEFAULT_KV_THRESHOLD) { _, _, it -> wrapped.kvThreshold(it) }

    /**
     * @see ThresholdRequestTracerConfig.Builder.queryThreshold
     */
    public var queryThreshold: Duration
            by observable(DEFAULT_QUERY_THRESHOLD) { _, _, it -> wrapped.queryThreshold(it) }

    /**
     * @see ThresholdRequestTracerConfig.Builder.viewThreshold
     */
    public var viewThreshold: Duration
            by observable(DEFAULT_VIEW_THRESHOLD) { _, _, it -> wrapped.viewThreshold(it) }

    /**
     * @see ThresholdRequestTracerConfig.Builder.searchThreshold
     */
    public var searchThreshold: Duration
            by observable(DEFAULT_SEARCH_THRESHOLD) { _, _, it -> wrapped.searchThreshold(it) }

    /**
     * @see ThresholdRequestTracerConfig.Builder.analyticsThreshold
     */
    public var analyticsThreshold: Duration
            by observable(DEFAULT_ANALYTICS_THRESHOLD) { _, _, it -> wrapped.analyticsThreshold(it) }
}
