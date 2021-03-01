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

import com.couchbase.client.core.cnc.EventBus
import com.couchbase.client.core.cnc.Meter
import com.couchbase.client.core.cnc.RequestTracer
import com.couchbase.client.core.encryption.CryptoManager
import com.couchbase.client.core.env.CoreEnvironment
import com.couchbase.client.core.env.PropertyLoader
import com.couchbase.client.kotlin.annotations.UncommittedApi
import com.couchbase.client.kotlin.annotations.VolatileApi
import com.couchbase.client.kotlin.codec.JacksonJsonSerializer
import com.couchbase.client.kotlin.codec.JsonSerializer
import com.couchbase.client.kotlin.codec.JsonTranscoder
import com.couchbase.client.kotlin.codec.Transcoder
import com.couchbase.client.kotlin.env.dsl.clusterEnvironment
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.jsonMapper
import reactor.core.scheduler.Scheduler

public interface ClusterPropertyLoader : PropertyLoader<ClusterEnvironment.Builder>

/**
 * Resources and configuration for connecting to a Couchbase cluster.
 *
 * Create a new instance using the [clusterEnvironment] DSL builder.
 *
 * Alternatively, call [ClusterEnvironment.builder] and configure it
 * using the builder's Java API.
 */
public class ClusterEnvironment private constructor(builder: Builder) : CoreEnvironment(builder) {
    private val jsonSerializer: JsonSerializer
    private val transcoder: Transcoder
    private val cryptoManager: CryptoManager?

    init {
        this.cryptoManager = builder.cryptoManager
        jsonSerializer = builder.jsonSerializer ?: JacksonJsonSerializer(jsonMapper {
            addModule(Jdk8Module())
            addModule(KotlinModule())
        })
        transcoder = builder.transcoder ?: JsonTranscoder(jsonSerializer)
    }

    public companion object {
        /**
         * Creates a [Builder] to customize the properties of the environment.
         *
         * @return the [Builder] to customize.
         */
        public fun builder(): Builder {
            return Builder()
        }
    }

    override fun defaultAgentTitle(): String {
        return "kotlin"
    }

    public class Builder internal constructor() : CoreEnvironment.Builder<Builder>() {
        internal var jsonSerializer: JsonSerializer? = null
        internal var transcoder: Transcoder? = null
        internal var cryptoManager: CryptoManager? = null

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

        @UncommittedApi
        override fun eventBus(eventBus: EventBus?): Builder {
            return super.eventBus(eventBus)
        }

        @UncommittedApi
        override fun scheduler(scheduler: Scheduler?): Builder {
            return super.scheduler(scheduler)
        }

        @VolatileApi
        override fun requestTracer(requestTracer: RequestTracer?): Builder {
            return super.requestTracer(requestTracer)
        }

        @VolatileApi
        override fun meter(meter: Meter?): Builder {
            return super.meter(meter)
        }

        /**
         * Creates a [ClusterEnvironment] from the values of this builder.
         *
         * @return the created cluster environment.
         */
        override fun build(): ClusterEnvironment = ClusterEnvironment(this)
    }

}
