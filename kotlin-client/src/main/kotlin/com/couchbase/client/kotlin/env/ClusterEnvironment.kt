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

import com.couchbase.client.core.encryption.CryptoManager
import com.couchbase.client.core.env.CoreEnvironment
import com.couchbase.client.core.env.PropertyLoader
import com.couchbase.client.kotlin.codec.JacksonJsonSerializer
import com.couchbase.client.kotlin.codec.JsonSerializer
import com.couchbase.client.kotlin.codec.JsonTranscoder
import com.couchbase.client.kotlin.codec.Transcoder
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.jsonMapper

public interface ClusterPropertyLoader : PropertyLoader<ClusterEnvironment.Builder>

/**
 * Resources and configuration for connecting to a Couchbase cluster.
 *
 * Create new instances from a [ClusterEnvironment.Builder].
 * Get a builder from [ClusterEnvironment.builder].
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
        public fun builder(): ClusterEnvironment.Builder {
            return ClusterEnvironment.Builder()
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
         */
        public fun jsonSerializer(jsonSerializer: JsonSerializer?): Builder {
            this.jsonSerializer = jsonSerializer
            return this
        }

        /**
         * Specifies the default transcoder for all KV operations.
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

        /**
         * Creates a [ClusterEnvironment] from the values of this builder.
         *
         * @return the created cluster environment.
         */
        override fun build(): ClusterEnvironment = ClusterEnvironment(this)
    }

}
