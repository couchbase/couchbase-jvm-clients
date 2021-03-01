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

import com.couchbase.client.core.env.AggregatingMeterConfig
import com.couchbase.client.core.env.AggregatingMeterConfig.Defaults.DEFAULT_EMIT_INTERVAL
import com.couchbase.client.core.env.AggregatingMeterConfig.Defaults.DEFAULT_ENABLED
import java.time.Duration
import kotlin.properties.Delegates.observable

/**
 * DSL counterpart to [AggregatingMeterConfig.Builder].
 */
@ClusterEnvironmentDslMarker
public class AggregatingMeterConfigDslBuilder(private val wrapped: AggregatingMeterConfig.Builder) {
    /**
     * @see AggregatingMeterConfig.Builder.emitInterval
     */
    public var emitInterval: Duration
            by observable(DEFAULT_EMIT_INTERVAL) { _, _, it -> wrapped.emitInterval(it) }

    /**
     * @see AggregatingMeterConfig.Builder.enabled
     */
    public var enabled: Boolean
            by observable(DEFAULT_ENABLED) { _, _, it -> wrapped.enabled(it) }
}
