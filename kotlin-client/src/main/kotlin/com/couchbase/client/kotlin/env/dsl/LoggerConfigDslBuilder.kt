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

import com.couchbase.client.core.cnc.LoggingEventConsumer
import com.couchbase.client.core.env.LoggerConfig
import com.couchbase.client.core.env.LoggerConfig.Defaults.*
import java.util.logging.Level
import kotlin.properties.Delegates.observable

/**
 * DSL counterpart to [LoggerConfig.Builder].
 */
@ClusterEnvironmentDslMarker
public class LoggerConfigDslBuilder(private val wrapped: LoggerConfig.Builder) {

    internal var customLogger: LoggingEventConsumer.Logger?
            by observable(null) { _, _, it -> wrapped.customLogger(it) }

    /**
     * @see LoggerConfig.Builder.fallbackToConsole
     */
    public var fallbackToConsole: Boolean
            by observable(DEFAULT_FALLBACK_TO_CONSOLE) { _, _, it -> wrapped.fallbackToConsole(it) }

    /**
     * @see LoggerConfig.Builder.consoleLogLevel
     */
    public var consoleLogLevel: Level
            by observable(DEFAULT_CONSOLE_LOG_LEVEL) { _, _, it -> wrapped.consoleLogLevel(it) }
    /**
     * @see LoggerConfig.Builder.disableSlf4J
     */
    public var disableSlf4J: Boolean
            by observable(DEFAULT_DISABLE_SLF4J) { _, _, it -> wrapped.disableSlf4J(it) }

    /**
     * @see LoggerConfig.Builder.enableDiagnosticContext
     */
    public var enableDiagnosticContext: Boolean
            by observable(DEFAULT_DIAGNOSTIC_CONTEXT_ENABLED) { _, _, it -> wrapped.enableDiagnosticContext(it) }
}
