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

import com.couchbase.client.core.env.TimeoutConfig
import java.time.Duration
import kotlin.properties.Delegates.observable

/**
 * DSL counterpart to [TimeoutConfig.Builder].
 */
@ClusterEnvironmentDslMarker
public class TimeoutConfigDslBuilder(private val wrapped: TimeoutConfig.Builder) {
    /**
     * @see TimeoutConfig.Builder.kvTimeout
     */
    public var kvTimeout: Duration
            by observable(TimeoutConfig.DEFAULT_KV_TIMEOUT) { _, _, it -> wrapped.kvTimeout(it) }

    /**
     * @see TimeoutConfig.Builder.kvDurableTimeout
     */
    public var kvDurableTimeout: Duration
            by observable(TimeoutConfig.DEFAULT_KV_DURABLE_TIMEOUT) { _, _, it -> wrapped.kvDurableTimeout(it) }

    /**
     * @see TimeoutConfig.Builder.managementTimeout
     */
    public var managementTimeout: Duration
            by observable(TimeoutConfig.DEFAULT_MANAGEMENT_TIMEOUT) { _, _, it -> wrapped.managementTimeout(it) }

    /**
     * @see TimeoutConfig.Builder.queryTimeout
     */
    public var queryTimeout: Duration
            by observable(TimeoutConfig.DEFAULT_QUERY_TIMEOUT) { _, _, it -> wrapped.queryTimeout(it) }

    /**
     * @see TimeoutConfig.Builder.viewTimeout
     */
    public var viewTimeout: Duration
            by observable(TimeoutConfig.DEFAULT_VIEW_TIMEOUT) { _, _, it -> wrapped.viewTimeout(it) }

    /**
     * @see TimeoutConfig.Builder.searchTimeout
     */
    public var searchTimeout: Duration
            by observable(TimeoutConfig.DEFAULT_SEARCH_TIMEOUT) { _, _, it -> wrapped.searchTimeout(it) }

    /**
     * @see TimeoutConfig.Builder.analyticsTimeout
     */
    public var analyticsTimeout: Duration
            by observable(TimeoutConfig.DEFAULT_ANALYTICS_TIMEOUT) { _, _, it -> wrapped.analyticsTimeout(it) }

    /**
     * @see TimeoutConfig.Builder.connectTimeout
     */
    public var connectTimeout: Duration
            by observable(TimeoutConfig.DEFAULT_CONNECT_TIMEOUT) { _, _, it -> wrapped.connectTimeout(it) }

    /**
     * @see TimeoutConfig.Builder.disconnectTimeout
     */
    public var disconnectTimeout: Duration
            by observable(TimeoutConfig.DEFAULT_DISCONNECT_TIMEOUT) { _, _, it -> wrapped.disconnectTimeout(it) }
}
