/*
 * Copyright (c) 2021 Couchbase, Inc.
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

package com.couchbase.client.kotlin

import com.couchbase.client.core.Core
import com.couchbase.client.core.config.BucketConfig
import com.couchbase.client.core.diagnostics.ClusterState
import com.couchbase.client.core.diagnostics.WaitUntilReadyHelper
import com.couchbase.client.core.error.UnambiguousTimeoutException
import com.couchbase.client.core.io.CollectionIdentifier.DEFAULT_SCOPE
import com.couchbase.client.core.service.ServiceType
import com.couchbase.client.core.util.BucketConfigUtil
import com.couchbase.client.kotlin.annotations.VolatileCouchbaseApi
import com.couchbase.client.kotlin.env.ClusterEnvironment
import com.couchbase.client.kotlin.internal.toOptional
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.future.await
import kotlinx.coroutines.reactive.asFlow
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

public class Bucket internal constructor(
    public val name: String,
    public val cluster: Cluster,
    internal val core: Core,
) {
    internal val env = core.context().environment() as ClusterEnvironment

    private val scopeCache = ConcurrentHashMap<String, Scope>()

    /**
     * Returns the default collection in this bucket's default scope.
     */
    public fun defaultCollection(): Collection = defaultScope().defaultCollection()

    /**
     * Returns the requested collection in the bucket's default scope.
     */
    public fun collection(name: String): Collection = defaultScope().collection(name)

    /**
     * Returns this bucket's default scope.
     */
    public fun defaultScope(): Scope = scope(DEFAULT_SCOPE)

    /**
     * Returns the requested scope.
     */
    public fun scope(name: String): Scope =
        scopeCache.computeIfAbsent(name) { Scope(name, this) }

    /**
     * Waits until SDK bootstrap is complete and the desired [ClusterState]
     * is observed.
     *
     * Calling this method is optional. Without it, operations performed
     * before the bucket is ready may take longer than usual, since
     * the SDK needs to load the bucket config from the server first.
     *
     * @param timeout the maximum time to wait for readiness.
     * @param services the service types to wait for, or empty set to wait for all services.
     * @throws UnambiguousTimeoutException if not ready before timeout
     */
    public suspend fun waitUntilReady(
        timeout: Duration,
        services: Set<ServiceType> = emptySet(),
        desiredState: ClusterState = ClusterState.ONLINE,
    ): Bucket {
        WaitUntilReadyHelper.waitUntilReady(core, services, timeout, desiredState, name.toOptional()).await()
        return this
    }

    @VolatileCouchbaseApi
    public suspend fun config(timeout: Duration): BucketConfig =
        core.clusterConfig().bucketConfig(name)
            ?: BucketConfigUtil.waitForBucketConfig(core, name, timeout).asFlow().single()
}
