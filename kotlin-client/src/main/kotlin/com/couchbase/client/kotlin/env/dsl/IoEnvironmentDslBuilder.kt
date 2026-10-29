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

import com.couchbase.client.core.env.IoEnvironment
import com.couchbase.client.core.env.IoEnvironment.DEFAULT_EVENT_LOOP_THREAD_COUNT
import com.couchbase.client.core.env.IoEnvironment.DEFAULT_NATIVE_IO_ENABLED
import kotlin.properties.Delegates.observable

private const val eventLoopDeprecationMessage: String = "This method may be removed in a future version of the SDK. Netty classes might not always be part of the SDK's public API, and a future version might use a different I/O engine. There is no direct replacement. To change how many threads the SDK uses for I/O, please use [eventLoopThreadCount] instead."

/**
 * DSL counterpart to [IoEnvironment.Builder].
 */
@ClusterEnvironmentDslMarker
public class IoEnvironmentDslBuilder(private val wrapped: IoEnvironment.Builder) {
    /**
     * @see IoEnvironment.Builder.enableNativeIo
     */
    public var enableNativeIo: Boolean
            by observable(DEFAULT_NATIVE_IO_ENABLED) { _, _, it -> wrapped.enableNativeIo(it) }

    /**
     * @see IoEnvironment.Builder.eventLoopThreadCount
     */
    public var eventLoopThreadCount: Int
            by observable(DEFAULT_EVENT_LOOP_THREAD_COUNT) { _, _, it -> wrapped.eventLoopThreadCount(it) }
}
