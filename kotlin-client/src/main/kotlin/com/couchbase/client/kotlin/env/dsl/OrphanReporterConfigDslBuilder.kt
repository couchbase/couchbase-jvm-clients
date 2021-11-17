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

import com.couchbase.client.core.env.OrphanReporterConfig
import com.couchbase.client.core.env.OrphanReporterConfig.Defaults.DEFAULT_EMIT_INTERVAL
import com.couchbase.client.core.env.OrphanReporterConfig.Defaults.DEFAULT_ENABLED
import com.couchbase.client.core.env.OrphanReporterConfig.Defaults.DEFAULT_QUEUE_LENGTH
import com.couchbase.client.core.env.OrphanReporterConfig.Defaults.DEFAULT_SAMPLE_SIZE
import kotlin.properties.Delegates.observable
import kotlin.time.Duration
import kotlin.time.toJavaDuration
import kotlin.time.toKotlinDuration

/**
 * DSL counterpart to [OrphanReporterConfig.Builder].
 */
@ClusterEnvironmentDslMarker
public class OrphanReporterConfigDslBuilder(private val wrapped: OrphanReporterConfig.Builder) {
    /**
     * @see OrphanReporterConfig.Builder.emitInterval
     */
    public var emitInterval: Duration
            by observable(DEFAULT_EMIT_INTERVAL.toKotlinDuration()) { _, _, it -> wrapped.emitInterval(it.toJavaDuration()) }

    /**
     * @see OrphanReporterConfig.Builder.sampleSize
     */
    public var sampleSize: Int
            by observable(DEFAULT_SAMPLE_SIZE) { _, _, it -> wrapped.sampleSize(it) }

    /**
     * @see OrphanReporterConfig.Builder.queueLength
     */
    public var queueLength: Int
            by observable(DEFAULT_QUEUE_LENGTH) { _, _, it -> wrapped.queueLength(it) }

    /**
     * @see OrphanReporterConfig.Builder.enabled
     */
    public var enabled: Boolean
            by observable(DEFAULT_ENABLED) { _, _, it -> wrapped.enabled(it) }
}
