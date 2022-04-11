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

package com.couchbase.client.kotlin.internal

import com.couchbase.client.core.error.TimeoutException
import com.couchbase.client.core.util.CbThrowables
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import kotlinx.coroutines.withTimeout
import reactor.core.publisher.Mono
import java.nio.charset.StandardCharsets.UTF_8
import kotlin.time.Duration

internal fun ByteArray.toStringUtf8() = toString(UTF_8)

internal fun <T> T?.toOptional() = java.util.Optional.ofNullable(this)

internal suspend fun Mono<Void>.await() = toFuture().await()

internal fun MutableMap<String, Any?>.putIfNotEmpty(key: String, value: Collection<*>) {
    if (value.isNotEmpty()) put(key, value)
}

internal fun MutableMap<String, Any?>.putIfNotEmpty(key: String, value: Map<*,*>) {
    if (value.isNotEmpty()) put(key, value)
}

internal fun MutableMap<String, Any?>.putIfTrue(key: String, value: Boolean) {
    if (value) put(key, true)
}

internal fun MutableMap<String, Any?>.putIfFalse(key: String, value: Boolean) {
    if (!value) put(key, false)
}

internal fun MutableMap<String, Any?>.putIfNotNull(key: String, value: Any?) {
    if (value != null) put(key, value)
}

internal fun MutableMap<String, Any?>.putIfNotZero(key: String, value: Int) {
    if (value != 0) put(key, value)
}

/**
 * Walks the causal chain of the throwable (starting with the throwable itself)
 * and returns the first throwable that is an instance of [T].
 */
internal inline fun <reified T : Throwable> Throwable.findCause(): T? {
    return CbThrowables.findCause(this, T::class.java).orElse(null)
}

/**
 * Returns true if the throwable or any throwable in its causal chain
 * is an instance of [T].
 */
internal inline fun <reified T : Throwable> Throwable.hasCause(): Boolean {
    return findCause<T>() != null
}

/**
 * The presence of this type in a method signature indicates
 * callers must use named arguments for all subsequent parameters.
 */
public class MustUseNamedArguments private constructor()
