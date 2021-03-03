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

import com.couchbase.client.core.env.IoConfig
import com.couchbase.client.core.env.IoConfig.DEFAULT_CONFIG_IDLE_REDIAL_TIMEOUT
import com.couchbase.client.core.env.IoConfig.DEFAULT_CONFIG_POLL_INTERVAL
import com.couchbase.client.core.env.IoConfig.DEFAULT_DNS_SRV_ENABLED
import com.couchbase.client.core.env.IoConfig.DEFAULT_IDLE_HTTP_CONNECTION_TIMEOUT
import com.couchbase.client.core.env.IoConfig.DEFAULT_MAX_HTTP_CONNECTIONS
import com.couchbase.client.core.env.IoConfig.DEFAULT_MUTATION_TOKENS_ENABLED
import com.couchbase.client.core.env.IoConfig.DEFAULT_NETWORK_RESOLUTION
import com.couchbase.client.core.env.IoConfig.DEFAULT_NUM_KV_CONNECTIONS
import com.couchbase.client.core.env.IoConfig.DEFAULT_TCP_KEEPALIVE_ENABLED
import com.couchbase.client.core.env.IoConfig.DEFAULT_TCP_KEEPALIVE_TIME
import com.couchbase.client.core.env.NetworkResolution
import com.couchbase.client.core.service.ServiceType
import java.time.Duration
import kotlin.properties.Delegates.observable

/**
 * DSL counterpart to [IoConfig.Builder].
 */
@ClusterEnvironmentDslMarker
public class IoConfigDslBuilder(private val wrapped: IoConfig.Builder) {
    /**
     * @see IoConfig.Builder.enableMutationTokens
     */
    public var enableMutationTokens: Boolean
            by observable(DEFAULT_MUTATION_TOKENS_ENABLED) { _, _, it -> wrapped.enableMutationTokens(it) }

    /**
     * @see IoConfig.Builder.configPollInterval
     */
    public var configPollInterval: Duration
            by observable(DEFAULT_CONFIG_POLL_INTERVAL) { _, _, it -> wrapped.configPollInterval(it) }

    /**
     * @see IoConfig.Builder.networkResolution
     */
    public var networkResolution: NetworkResolution
            by observable(DEFAULT_NETWORK_RESOLUTION) { _, _, it -> wrapped.networkResolution(it) }

    /**
     * @see IoConfig.Builder.captureTraffic
     */
    public fun captureTraffic(vararg services: ServiceType) {
        wrapped.captureTraffic(*services)
    }

    /**
     * @see IoConfig.Builder.enableDnsSrv
     */
    public var enableDnsSrv: Boolean
            by observable(DEFAULT_DNS_SRV_ENABLED) { _, _, it -> wrapped.enableDnsSrv(it) }

    /**
     * @see IoConfig.Builder.enableTcpKeepAlives
     */
    public var enableTcpKeepAlives: Boolean
            by observable(DEFAULT_TCP_KEEPALIVE_ENABLED) { _, _, it -> wrapped.enableTcpKeepAlives(it) }

    /**
     * @see IoConfig.Builder.tcpKeepAliveTime
     */
    public var tcpKeepAliveTime: Duration
            by observable(DEFAULT_TCP_KEEPALIVE_TIME) { _, _, it -> wrapped.tcpKeepAliveTime(it) }

    /**
     * @see IoConfig.Builder.numKvConnections
     */
    public var numKvConnections: Int
            by observable(DEFAULT_NUM_KV_CONNECTIONS) { _, _, it -> wrapped.numKvConnections(it) }

    /**
     * @see IoConfig.Builder.maxHttpConnections
     */
    public var maxHttpConnections: Int
            by observable(DEFAULT_MAX_HTTP_CONNECTIONS) { _, _, it -> wrapped.maxHttpConnections(it) }

    /**
     * @see IoConfig.Builder.idleHttpConnectionTimeout
     */
    public var idleHttpConnectionTimeout: Duration
            by observable(DEFAULT_IDLE_HTTP_CONNECTION_TIMEOUT) { _, _, it -> wrapped.idleHttpConnectionTimeout(it) }

    /**
     * @see IoConfig.Builder.configIdleRedialTimeout
     */
    public var configIdleRedialTimeout: Duration
            by observable(DEFAULT_CONFIG_IDLE_REDIAL_TIMEOUT) { _, _, it -> wrapped.configIdleRedialTimeout(it) }

    /**
     * Apply the given configuration to all circuit breakers.
     */
    public fun allCircuitBreakers(initializer: CircuitBreakerConfigDslBuilder.() -> Unit) {
        kvCircuitBreaker(initializer)
        queryCircuitBreaker(initializer)
        viewCircuitBreaker(initializer)
        searchCircuitBreaker(initializer)
        analyticsCircuitBreaker(initializer)
        managerCircuitBreaker(initializer)
    }

    private var kvCircuitBreakerConfigBuilder = CircuitBreakerConfigDslBuilder(wrapped.kvCircuitBreakerConfig())
    public fun kvCircuitBreaker(initializer: CircuitBreakerConfigDslBuilder.() -> Unit) {
        kvCircuitBreakerConfigBuilder.initializer()
    }

    private var queryCircuitBreakerConfigBuilder = CircuitBreakerConfigDslBuilder(wrapped.queryCircuitBreakerConfig())
    public fun queryCircuitBreaker(initializer: CircuitBreakerConfigDslBuilder.() -> Unit) {
        queryCircuitBreakerConfigBuilder.initializer()
    }

    private var viewCircuitBreakerConfigBuilder = CircuitBreakerConfigDslBuilder(wrapped.viewCircuitBreakerConfig())
    public fun viewCircuitBreaker(initializer: CircuitBreakerConfigDslBuilder.() -> Unit) {
        viewCircuitBreakerConfigBuilder.initializer()
    }

    private var searchCircuitBreakerConfigBuilder = CircuitBreakerConfigDslBuilder(wrapped.searchCircuitBreakerConfig())
    public fun searchCircuitBreaker(initializer: CircuitBreakerConfigDslBuilder.() -> Unit) {
        searchCircuitBreakerConfigBuilder.initializer()
    }

    private var analyticsCircuitBreakerConfigBuilder =
        CircuitBreakerConfigDslBuilder(wrapped.analyticsCircuitBreakerConfig())

    public fun analyticsCircuitBreaker(initializer: CircuitBreakerConfigDslBuilder.() -> Unit) {
        analyticsCircuitBreakerConfigBuilder.initializer()
    }

    private var managerCircuitBreakerConfigBuilder =
        CircuitBreakerConfigDslBuilder(wrapped.managerCircuitBreakerConfig())

    public fun managerCircuitBreaker(initializer: CircuitBreakerConfigDslBuilder.() -> Unit) {
        managerCircuitBreakerConfigBuilder.initializer()
    }
}
