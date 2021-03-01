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

import com.couchbase.client.core.env.CompressionConfig
import com.couchbase.client.core.env.CompressionConfig.DEFAULT_ENABLED
import com.couchbase.client.core.env.CompressionConfig.DEFAULT_MIN_RATIO
import com.couchbase.client.core.env.CompressionConfig.DEFAULT_MIN_SIZE
import kotlin.properties.Delegates.observable

/**
 * DSL counterpart to [CompressionConfig.Builder].
 */
@ClusterEnvironmentDslMarker
public class CompressionConfigDslBuilder(private val wrapped: CompressionConfig.Builder) {
    /**
     * @see CompressionConfig.Builder.enabled
     */
    public var enable: Boolean
            by observable(DEFAULT_ENABLED) { _, _, it -> wrapped.enable(it) }

    /**
     * @see CompressionConfig.Builder.minRatio
     */
    public var minRatio: Double
            by observable(DEFAULT_MIN_RATIO) { _, _, it -> wrapped.minRatio(it) }

    /**
     * @see CompressionConfig.Builder.minSize
     */
    public var minSize: Int
            by observable(DEFAULT_MIN_SIZE) { _, _, it -> wrapped.minSize(it) }
}
