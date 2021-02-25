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

package com.couchbase.client.kotlin.codec

import com.couchbase.client.core.error.DecodingFailureException
import com.couchbase.client.core.error.InvalidArgumentException
import com.couchbase.client.kotlin.internal.toStringUtf8

public object RawStringTranscoder : Transcoder {
    override fun <T> doEncode(input: T, type: TypeRef<T>): Content {
        return when (input) {
            is String -> Content.string(input)
            else -> throw InvalidArgumentException.fromMessage(
                "Only String is supported for the RawStringTranscoder!"
            )
        }
    }

    @Suppress("UNCHECKED_CAST")
    override fun <T> decode(content: Content, type: TypeRef<T>): T {
        return when (type.type) {
            String::class.java -> content.bytes.toStringUtf8() as T
            else -> throw DecodingFailureException(
                "RawStringTranscoder can only decode into String!"
            )
        }
    }
}
