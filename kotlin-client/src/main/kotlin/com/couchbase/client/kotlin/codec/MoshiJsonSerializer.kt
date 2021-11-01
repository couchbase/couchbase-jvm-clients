/*
 * Copyright 2021 Couchbase, Inc.
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

package com.couchbase.client.kotlin.codec

import com.squareup.moshi.JsonAdapter
import com.squareup.moshi.Moshi
import okio.Buffer

/**
 * A JSON serializer backed by Moshi.
 *
 * Requires adding Moshi 1.x as an additional dependency of your project.
 * Maven coordinates of the additional dependency:
 *
 * ```
 * <dependency>
 *     <groupId>com.squareup.moshi</groupId>
 *     <artifactId>moshi-kotlin</artifactId>
 *     <version>${moshi.version}</version>
 * </dependency>
 * ```
 *
 * Sample code for configuring the Couchbase cluster environment using Moshi
 * in reflection mode:
 *```
 * val moshi = Moshi.Builder().add(KotlinJsonAdapterFactory()).build()
 * val cluster = Cluster.connect("localhost", "Administrator", "password") {
 *     jsonSerializer = MoshiJsonSerializer(moshi)
 * }
 * ```
 *
 * To use Moshi's code generation mode, don't add `KotlinJsonAdapterFactory`, and instead
 * look to the Moshi documentation for details on how to run the code generator
 * as part of your build process.
 *
 * The `MoshiJsonSerializer` constructor takes an optional lambda for customizing the Moshi
 * JsonAdapter. This lambda is invoked in a context where "this" refers to the
 * default JsonAdapter. The lambda returns the JsonAdapter to use.
 * For example, to configure Moshi to serialize nulls:
 *
 * ```
 *     jsonSerializer = MoshiSerializer(moshi) { serializeNulls() }
 * ```
 */
public class MoshiJsonSerializer(
    private val moshi: Moshi,
    private val customize: JsonAdapter<*>.(type: TypeRef<*>) -> JsonAdapter<*> = { this },
) : JsonSerializer {

    override fun <T> serialize(value: T, type: TypeRef<T>): ByteArray {
        @Suppress("UNCHECKED_CAST")
        val adapter = customize(moshi.adapter<T>(type.type), type) as JsonAdapter<T>
        val buffer = Buffer()
        adapter.toJson(buffer, value)
        return buffer.readByteArray()
    }

    override fun <T> deserialize(json: ByteArray, type: TypeRef<T>): T {
        val adapter = customize(moshi.adapter<T>(type.type), type)
        val buffer = Buffer()
        buffer.write(json)

        @Suppress("UNCHECKED_CAST")
        val result: T = adapter.fromJson(buffer) as T

        if (result == null && !type.nullable) {
            throw NullPointerException("Can't deserialize null value into non-nullable type $type")
        }

        return result
    }
}
