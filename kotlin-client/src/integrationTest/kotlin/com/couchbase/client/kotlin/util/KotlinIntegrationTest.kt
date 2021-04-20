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

package com.couchbase.client.kotlin.util

import com.couchbase.client.core.env.Authenticator
import com.couchbase.client.core.env.PasswordAuthenticator
import com.couchbase.client.core.env.SeedNode
import com.couchbase.client.kotlin.Cluster
import com.couchbase.client.kotlin.env.dsl.ClusterEnvironmentConfigBlock
import com.couchbase.client.kotlin.internal.toOptional
import com.couchbase.client.test.ClusterAwareIntegrationTest
import com.couchbase.client.test.Services
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Timeout
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import kotlin.contracts.ExperimentalContracts
import kotlin.contracts.InvocationKind
import kotlin.contracts.contract

@Timeout(value = 10, unit = TimeUnit.MINUTES)
internal open class KotlinIntegrationTest : ClusterAwareIntegrationTest() {

    private val lazyCluster = lazy { connect() }
    protected val cluster by lazyCluster

    protected val bucket by lazy {
        runBlocking {
            cluster.bucket(config().bucketname())
                // It should not take this long, but the Jenkins box...
                .waitUntilReady(Duration.ofSeconds(30))
        }
    }

    protected val collection by lazy { bucket.defaultCollection() }

    private val nextIdCounter = AtomicLong()
    private val nextIdBase = "integration-test-${System.currentTimeMillis()}-${UUID.randomUUID()}-"
    protected fun nextId() = nextIdBase + nextIdCounter.getAndIncrement()

    @AfterAll
    fun afterAll() = runBlocking {
        if (lazyCluster.isInitialized()) cluster.disconnect()
    }

    fun authenticator(): Authenticator {
        return PasswordAuthenticator.create(config().adminUsername(), config().adminPassword())
    }

    val connectionString: String
        get() = seedNodes.joinToString(",") {
            if (it.kvPort().isPresent) it.address() + ":" + it.kvPort().get()
            else it.address()
        }

    private val seedNodes: Set<SeedNode>
        get() = config().nodes().map {
            SeedNode.create(
                it.hostname(),
                it.ports()[Services.KV].toOptional(),
                it.ports()[Services.MANAGER].toOptional(),
            )
        }.toSet()

    val authenticator: Authenticator
        get() = PasswordAuthenticator.create(config().adminUsername(), config().adminPassword())

    fun connect(envConfig: ClusterEnvironmentConfigBlock = {}) =
        Cluster.connect(connectionString, authenticator, envConfig)
}

/**
 * Disconnects the cluster after executing the given block.
 */
internal inline fun <R> Cluster.use(block: (Cluster) -> R) =
    use(block, { runBlocking { disconnect() } })

/**
 * A version of `use` that is not limited to Closeables.
 */
@OptIn(ExperimentalContracts::class)
internal inline fun <T, R> T.use(block: (T) -> R, cleanupBlock: T.() -> Unit): R {
    contract {
        callsInPlace(block, InvocationKind.EXACTLY_ONCE)
        callsInPlace(cleanupBlock, InvocationKind.EXACTLY_ONCE)
    }
    var exception: Throwable? = null
    try {
        return block(this)
    } catch (e: Throwable) {
        exception = e
        throw e
    } finally {
        cleanupFinally(this, cleanupBlock, exception)
    }
}

@OptIn(ExperimentalContracts::class)
private inline fun <T> cleanupFinally(target: T, cleanupBlock: T.() -> Unit, cause: Throwable?): Unit {
    contract {
        callsInPlace(cleanupBlock, InvocationKind.EXACTLY_ONCE)
    }
    return when {
        target == null -> Unit
        cause == null -> target.cleanupBlock()
        else ->
            try {
                target.cleanupBlock()
            } catch (closeException: Throwable) {
                cause.addSuppressed(closeException)
            }
    }
}

/**
 * Sets the system property for the duration of the block.
 * Restores it afterwords. Concurrent test execution can mess this up.
 */
internal fun <T> withSystemProperty(name: String, value: String, block: () -> T): T {
    val previousValue = System.setProperty(name, value)
    try {
        return block()
    } finally {
        if (previousValue == null) System.clearProperty(name)
        else System.setProperty(name, previousValue)
    }
}
