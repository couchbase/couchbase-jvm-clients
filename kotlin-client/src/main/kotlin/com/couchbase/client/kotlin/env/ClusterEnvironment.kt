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

package com.couchbase.client.kotlin.env

import com.couchbase.client.core.Core
import com.couchbase.client.core.annotation.Stability
import com.couchbase.client.core.cnc.EventBus
import com.couchbase.client.core.cnc.Meter
import com.couchbase.client.core.cnc.RequestTracer
import com.couchbase.client.core.encryption.CryptoManager
import com.couchbase.client.core.env.CoreEnvironment
import com.couchbase.client.core.env.PropertyLoader
import com.couchbase.client.core.env.TimeoutConfig
import com.couchbase.client.core.env.VersionAndGitHash
import com.couchbase.client.kotlin.Cluster
import com.couchbase.client.kotlin.CommonOptions
import com.couchbase.client.kotlin.codec.JacksonJsonSerializer
import com.couchbase.client.kotlin.codec.JsonSerializer
import com.couchbase.client.kotlin.codec.JsonTranscoder
import com.couchbase.client.kotlin.codec.Transcoder
import com.couchbase.client.kotlin.env.dsl.ClusterEnvironmentConfigBlock
import com.couchbase.client.kotlin.env.dsl.ClusterEnvironmentDslBuilder
import com.couchbase.client.kotlin.internal.await
import com.couchbase.client.kotlin.kv.Durability
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.jsonMapper
import reactor.core.scheduler.Scheduler
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration
import kotlin.time.toJavaDuration
import kotlin.time.toKotlinDuration

public interface ClusterPropertyLoader : PropertyLoader<ClusterEnvironment.Builder>

private val clientVersion = VersionAndGitHash.from(Cluster::class.java)

/**
 * Resources and configuration for connecting to a Couchbase cluster.
 *
 * You don't need to worry about creating an environment unless you want
 * to share a single environment between multiple clusters. In that case,
 * create an environment using a builder returned by
 * [ClusterEnvironment.builder].
 *
 * @see Cluster.connectUsingSharedEnvironment
 * @see ClusterEnvironment.builder
 */
public class ClusterEnvironment private constructor(builder: Builder) : CoreEnvironment(builder) {
    internal val jsonSerializer: JsonSerializer
    internal val transcoder: Transcoder
    internal val cryptoManager: CryptoManager?

    init {
        this.cryptoManager = builder.cryptoManager
        jsonSerializer = builder.jsonSerializer ?: JacksonJsonSerializer(jsonMapper {
            addModule(Jdk8Module())
            addModule(KotlinModule.Builder().build())
        })
        transcoder = builder.transcoder ?: JsonTranscoder(jsonSerializer)
    }

    public companion object {
        /**
         * Returns a new [ClusterEnvironment.Builder]. If you provide an
         * environment config DSL block, the returned builder has
         * settings configured by the block. Otherwise the builder has
         * default settings, which you can customize by calling methods
         * on the builder.
         *
         * Pass the returned builder to one of the [Cluster.connect] methods,
         * or call [build] and pass the resulting [ClusterEnvironment] to
         * [Cluster.connectUsingSharedEnvironment]. Remember: if you call
         * build() yourself, you are responsible for shutting down the
         * environment after disconnecting the clusters that share it.
         *
         * @param configBlock an optional lambda for configuring the new
         * builder using cluster environment config DSL.
         *
         * @sample com.couchbase.client.kotlin.samples.createBuilderWithDefaultSettings
         * @sample com.couchbase.client.kotlin.samples.preconfigureBuilderUsingDsl
         */
        public fun builder(configBlock: ClusterEnvironmentConfigBlock = {}): Builder {
            val builder = ClusterEnvironmentDslBuilder()
            builder.configBlock()
            return builder.toCore()
        }
    }

    /**
     * Pick your tagline.
     *
     * "A modern programming language that makes developers happier."
     *     -- [kotlinlang.org](https://kotlinlang.org)
     *
     * "Kotlin: So you don’t need a billion lines of code to get your shit done!"
     *     -- [Erik Meijer](https://youtu.be/NKeHrApPWlo?t=92s)
     */
    override fun defaultAgentTitle(): String {
        return "kotlin"
    }

    override fun clientVersionAndGitHash(): VersionAndGitHash {
        return clientVersion
    }

    /**
     * Shuts down this environment. Does the same thing as [shutdown],
     * but suspends instead of blocking.
     *
     * This should be the very last operation in the SDK shutdown process,
     * after all clients sharing this environment have disconnected.
     *
     * Note that once shut down, the environment cannot be restarted.
     */
    public suspend fun shutdownSuspend(timeout: Duration = timeoutConfig().disconnectTimeout().toKotlinDuration()) {
        shutdownReactive(timeout.toJavaDuration()).await()
    }

    /**
     * Used for configuring cluster environment settings, or creating
     * a shared cluster environment.
     *
     * Call [ClusterEnvironment.builder] to create a new instance.
     */
    public class Builder internal constructor() : CoreEnvironment.Builder<Builder>() {
        internal var jsonSerializer: JsonSerializer? = null
        internal var transcoder: Transcoder? = null
        internal var cryptoManager: CryptoManager? = null
        internal var used = AtomicBoolean()

        /**
         * Immediately loads the properties from the given loader into the environment.
         *
         * @param loader the loader to load the properties from.
         * @return this [Builder] for chaining purposes.
         */
        public fun load(loader: ClusterPropertyLoader): Builder {
            loader.load(this)
            return this
        }

        /**
         * Sets the default serializer for converting between JSON and Java objects.
         *
         * Defaults to a [JacksonJsonSerializer] with modules for Kotlin and JDK8.
         */
        public fun jsonSerializer(jsonSerializer: JsonSerializer?): Builder {
            this.jsonSerializer = jsonSerializer
            return this
        }

        /**
         * Specifies the default transcoder for all KV operations.
         *
         * Defaults to a [JsonTranscoder] backed by this environment's [JsonSerializer].
         *
         * @param transcoder the transcoder that should be used by default.
         * @return this [Builder] for chaining purposes.
         */
        public fun transcoder(transcoder: Transcoder?): Builder {
            this.transcoder = transcoder
            return this
        }

        /**
         * Sets the cryptography manager for Field-Level Encryption
         * (reading and writing encrypted document fields).
         *
         * Defaults to null, which disables encryption.
         *
         * Note: Use of the Field-Level Encryption functionality is
         * subject to the [Couchbase Inc. Enterprise Subscription License Agreement v7](https://www.couchbase.com/ESLA01162020)
         *
         * @param cryptoManager the manager to use, or null to disable encryption support.
         * @return this builder for chaining purposes.
         */
        public fun cryptoManager(cryptoManager: CryptoManager?): Builder {
            this.cryptoManager = cryptoManager
            return this
        }

        @Stability.Uncommitted
        override fun eventBus(eventBus: EventBus?): Builder {
            return super.eventBus(eventBus)
        }

        @Stability.Uncommitted
        override fun scheduler(scheduler: Scheduler?): Builder {
            return super.scheduler(scheduler)
        }

        @Stability.Uncommitted
        override fun schedulerThreadCount(schedulerThreadCount: Int): Builder {
            return super.schedulerThreadCount(schedulerThreadCount)
        }

        @Stability.Uncommitted
        override fun requestTracer(requestTracer: RequestTracer?): Builder {
            return super.requestTracer(requestTracer)
        }

        @Stability.Uncommitted
        override fun meter(meter: Meter?): Builder {
            return super.meter(meter)
        }

        /**
         * Creates a [ClusterEnvironment] from the values of this builder.
         *
         * IMPORTANT: If you call this method, you are responsible for
         * shutting down the environment when it is no longer needed.
         *
         * @return the created cluster environment.
         * @throws IllegalStateException if this method has already been called.
         * @see shutdownSuspend
         */
        override fun build(): ClusterEnvironment {
            // Prevent passing the same builder to Cluster.connect()
            // multiple times with different connection strings.
            // Don't want the connection string params to leak into the builder.
            check(used.compareAndSet(false, true)) {
                "ClusterEnvironment.Builder.build() may only be called once."
            }
            return ClusterEnvironment(this)
        }
    }

    internal fun CommonOptions.actualRetryStrategy() = retryStrategy ?: retryStrategy()
    internal fun CommonOptions.actualSpan(name: String) = requestTracer().requestSpan(name, parentSpan)
    internal fun CommonOptions.actualViewTimeout() = timeout?.toJavaDuration() ?: timeoutConfig().viewTimeout()
    internal fun CommonOptions.actualManagementTimeout() =
        timeout?.toJavaDuration() ?: timeoutConfig().managementTimeout().toKotlinDuration()

    internal fun CommonOptions.actualQueryTimeout() = timeout?.toJavaDuration() ?: timeoutConfig().queryTimeout()
    internal fun CommonOptions.actualAnalyticsTimeout() =
        timeout?.toJavaDuration() ?: timeoutConfig().analyticsTimeout()

    internal fun CommonOptions.actualKvTimeout(durability: Durability) =
        (timeout ?: timeoutConfig().kvTimeout(durability)).toJavaDuration()

    internal fun CommonOptions.actualKvScanTimeout() =
        (timeout?.toJavaDuration() ?: timeoutConfig().kvScanTimeout())

    internal fun CommonOptions.actualSearchTimeout() =
        timeout?.toJavaDuration() ?: timeoutConfig().searchTimeout()

    private fun TimeoutConfig.kvTimeout(durability: Durability) =
        (if (durability.isPersistent()) kvDurableTimeout() else kvTimeout()).toKotlinDuration()
}

internal val Core.env : ClusterEnvironment
    get() = context().environment() as ClusterEnvironment
