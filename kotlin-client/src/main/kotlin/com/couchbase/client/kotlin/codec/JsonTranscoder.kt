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

import com.couchbase.client.core.error.InvalidArgumentException

/**
 * Converts the value to and from JSON by delegating to a [JsonSerializer]
 */
public class JsonTranscoder(private val serializer: JsonSerializer) : Transcoder {
    override fun <T> doEncode(input: T, type: TypeRef<T>): Content {
        return when (input) {
            is ByteArray -> throw InvalidArgumentException.fromMessage(
                "JsonTranscoder can't encode ByteArray. To write arbitrary binary content, use RawBinaryTranscoder or pass Content.binary(myByteArray). To write pre-serialized JSON, use RawJsonTranscoder or pass Content.json(mySerializedJson)."
            )
            else -> Content.json(serializer.serialize(input, type))
        }
    }

    override fun <T> decode(content: Content, type: TypeRef<T>): T {
        return when (type.type) {
            ByteArray::class.java -> throw InvalidArgumentException.fromMessage(
                "JsonTranscoder can't decode to ByteArray. To access raw document content, use the 'content' property instead of calling 'contentAs<ByteArray>()'"
            )
            else -> serializer.deserialize(content.bytes, type)
        }
    }
}
