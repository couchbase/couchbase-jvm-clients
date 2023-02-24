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

package com.couchbase.client.kotlin.query

import com.couchbase.client.core.api.query.CoreQueryMetrics
import com.couchbase.client.core.classic.query.ClassicCoreQueryMetrics
import com.couchbase.client.core.json.Mapper
import java.util.concurrent.TimeUnit
import kotlin.time.Duration
import kotlin.time.toKotlinDuration

public class QueryMetrics internal constructor(
    internal val core: CoreQueryMetrics,
) {
    // Oops, this constructor is part of the public API :-(
    @Deprecated(message = "QueryMetrics constructor will be private in a future version.")
    public constructor(map: Map<String, Any?>) : this(ClassicCoreQueryMetrics(Mapper.encodeAsBytes(map)))

    @Deprecated(message = "QueryMetrics.map property will be removed in a future version.")
    public val map: Map<String, Any?>
        get() = mapOf(
            "elapsedTime" to core.elapsedTime().toGolangMicros(),
            "executionTime" to core.executionTime().toGolangMicros(),
            "sortCount" to core.sortCount(),
            "resultCount" to core.resultCount(),
            "resultSize" to core.resultSize(),
            "mutationCount" to core.mutationCount(),
            "errorCount" to core.errorCount(),
            "warningCount" to core.warningCount(),
        )

    public val elapsedTime: Duration
        get() = core.elapsedTime().toKotlinDuration()

    public val executionTime: Duration
        get() = core.executionTime().toKotlinDuration()

    public val sortCount: Long
        get() = core.sortCount()

    public val resultCount: Long
        get() = core.resultCount()

    public val resultSize: Long
        get() = core.resultSize()

    @Deprecated("This metric is no longer exposed.")
    public val usedMemory: Long
        get() = 0L

    @Deprecated("This metric is no longer exposed.")
    public val serviceLoad: Long
        get() = 0L

    public val mutationCount: Long
        get() = core.mutationCount()

    public val errorCount: Long
        get() = core.errorCount()

    public val warningCount: Long
        get() = core.warningCount()

    override fun toString(): String {
        return "QueryMetrics(elapsedTime=$elapsedTime, executionTime=$executionTime, sortCount=$sortCount, resultCount=$resultCount, resultSize=$resultSize, mutationCount=$mutationCount, errorCount=$errorCount, warningCount=$warningCount)"
    }
}

private fun java.time.Duration.toGolangMicros(): String {
    val micros = TimeUnit.NANOSECONDS.toMicros(toNanos());
    return "${micros}us"
}
