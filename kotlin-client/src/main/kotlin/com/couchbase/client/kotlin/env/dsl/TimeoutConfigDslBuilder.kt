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
import com.couchbase.client.kotlin.annotations.VolatileCouchbaseApi
import kotlin.properties.Delegates.observable
import kotlin.time.Duration
import kotlin.time.toJavaDuration
import kotlin.time.toKotlinDuration

/**
 * DSL counterpart to [TimeoutConfig.Builder].
 */
@ClusterEnvironmentDslMarker
public class TimeoutConfigDslBuilder(private val wrapped: TimeoutConfig.Builder) {
    /**
     * @see TimeoutConfig.Builder.kvTimeout
     */
    public var kvTimeout: Duration
            by observable(TimeoutConfig.DEFAULT_KV_TIMEOUT.toKotlinDuration()) { _, _, it -> wrapped.kvTimeout(it.toJavaDuration()) }

    /**
     * @see TimeoutConfig.Builder.kvDurableTimeout
     */
    public var kvDurableTimeout: Duration
            by observable(TimeoutConfig.DEFAULT_KV_DURABLE_TIMEOUT.toKotlinDuration()) { _, _, it ->
                wrapped.kvDurableTimeout(it.toJavaDuration())
            }

    /**
     * @see TimeoutConfig.Builder.kvScanTimeout
     */
    @VolatileCouchbaseApi
    public var kvScanTimeout: Duration
            by observable(TimeoutConfig.DEFAULT_KV_SCAN_TIMEOUT.toKotlinDuration()) { _, _, it ->
                wrapped.kvScanTimeout(it.toJavaDuration())
            }

    /**
     * @see TimeoutConfig.Builder.managementTimeout
     */
    public var managementTimeout: Duration
            by observable(TimeoutConfig.DEFAULT_MANAGEMENT_TIMEOUT.toKotlinDuration()) { _, _, it ->
                wrapped.managementTimeout(it.toJavaDuration())
            }

    /**
     * @see TimeoutConfig.Builder.eventingTimeout
     */
    public var eventingTimeout: Duration
            by observable(TimeoutConfig.DEFAULT_EVENTING_TIMEOUT.toKotlinDuration()) { _, _, it ->
                wrapped.eventingTimeout(it.toJavaDuration())
            }

    /**
     * @see TimeoutConfig.Builder.backupTimeout
     */
    public var backupTimeout: Duration
            by observable(TimeoutConfig.DEFAULT_BACKUP_TIMEOUT.toKotlinDuration()) { _, _, it ->
                wrapped.backupTimeout(it.toJavaDuration())
            }

    /**
     * @see TimeoutConfig.Builder.queryTimeout
     */
    public var queryTimeout: Duration
            by observable(TimeoutConfig.DEFAULT_QUERY_TIMEOUT.toKotlinDuration()) { _, _, it -> wrapped.queryTimeout(it.toJavaDuration()) }

    /**
     * @see TimeoutConfig.Builder.viewTimeout
     */
    public var viewTimeout: Duration
            by observable(TimeoutConfig.DEFAULT_VIEW_TIMEOUT.toKotlinDuration()) { _, _, it -> wrapped.viewTimeout(it.toJavaDuration()) }

    /**
     * @see TimeoutConfig.Builder.searchTimeout
     */
    public var searchTimeout: Duration
            by observable(TimeoutConfig.DEFAULT_SEARCH_TIMEOUT.toKotlinDuration()) { _, _, it ->
                wrapped.searchTimeout(it.toJavaDuration())
            }

    /**
     * @see TimeoutConfig.Builder.analyticsTimeout
     */
    public var analyticsTimeout: Duration
            by observable(TimeoutConfig.DEFAULT_ANALYTICS_TIMEOUT.toKotlinDuration()) { _, _, it ->
                wrapped.analyticsTimeout(it.toJavaDuration())
            }

    /**
     * @see TimeoutConfig.Builder.connectTimeout
     */
    public var connectTimeout: Duration
            by observable(TimeoutConfig.DEFAULT_CONNECT_TIMEOUT.toKotlinDuration()) { _, _, it ->
                wrapped.connectTimeout(it.toJavaDuration())
            }

    /**
     * @see TimeoutConfig.Builder.disconnectTimeout
     */
    public var disconnectTimeout: Duration
            by observable(TimeoutConfig.DEFAULT_DISCONNECT_TIMEOUT.toKotlinDuration()) { _, _, it ->
                wrapped.disconnectTimeout(it.toJavaDuration())
            }
}
