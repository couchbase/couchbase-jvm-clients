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

package com.couchbase.client.kotlin.analytics

import com.couchbase.client.core.util.Golang
import java.time.Duration

public class AnalyticsMetrics(
    public val map: Map<String, Any?>,
) {
    public val elapsedTime: Duration
        get() = getDuration("elapsedTime")

    public val executionTime: Duration
        get() = getDuration("executionTime")

    public val resultCount: Long
        get() = getLong("resultCount")

    public val resultSize: Long
        get() = getLong("resultSize")

    public val errorCount: Long
        get() = getLong("errorCount")

    public val warningCount: Long
        get() = getLong("warningCount")

    public val processedObjects: Long
        get() = getLong("processedObjects")

    private fun getDuration(key: String): Duration = Golang.parseDuration(map[key] as String? ?: "0")

    private fun getLong(key: String): Long = (map[key] as Number? ?: 0).toLong()

    override fun toString(): String {
        return "AnalyticsMetrics(map=$map)"
    }
}
