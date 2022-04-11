/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.kotlin.internal

import com.couchbase.client.core.error.TimeoutException
import com.couchbase.client.core.util.NanoTimestamp
import kotlinx.coroutines.delay
import kotlin.time.Duration
import kotlin.time.toJavaDuration

internal class RetryTimeoutException(
    message: String,
    val timeout: Duration,
    val lastError: Throwable,
) : TimeoutException(message, null)

internal suspend fun <T> retry(
    timeout: Duration,
    onlyIf: (Throwable) -> Boolean = { true },
    block: suspend () -> T,
): T {
    val javaTimeout = timeout.toJavaDuration()
    var lastError: Throwable

    val start = NanoTimestamp.now()
    var delayMillis = 50L

    do {
        try {
            return block()
        } catch (t: Throwable) {
            if (!onlyIf(t)) throw t;
            lastError = t
            delay(timeMillis = delayMillis)
            delayMillis = (delayMillis * 2).coerceAtMost(1000)
        }
    } while (start.elapsed() < javaTimeout)

    throw RetryTimeoutException("Retry timed out after $timeout", timeout, lastError)
}
