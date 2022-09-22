/*
 * Copyright 2022 Couchbase, Inc.
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

import com.couchbase.client.kotlin.annotations.VolatileCouchbaseApi
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.serializer
import java.lang.reflect.Type
import java.util.concurrent.ConcurrentHashMap

private val serializerCache = ConcurrentHashMap<Type, KSerializer<*>>()

/**
 * A JsonSerializer for integrating with `kotlinx.serialization`.
 *
 * **CAVEAT:** This class uses experimental Kotlin Serialization APIs,
 * and only works with non-nullable types. We plan to lift these limitations after
 * [kotlinx.serialization issue 1348](https://github.com/Kotlin/kotlinx.serialization/issues/1348)
 * is resolved.
 *
 * In the meantime, if you prefer not to rely on experimental APIs, or if you must read or write a nullable type,
 * we advise doing the serialization "manually" as shown in the following sample.
 * Note that you can still make [KotlinxSerializationJsonSerializer] the default serializer,
 * and override the serialization for nullable types as needed.
 *
 * @sample com.couchbase.client.kotlin.samples.configureKotlinxSerializationJsonSerializer
 * @sample com.couchbase.client.kotlin.samples.manualKotlinxSerialization
 */
@ExperimentalSerializationApi
@VolatileCouchbaseApi
public class KotlinxSerializationJsonSerializer(
    private val jsonFormat: Json = Json
) : JsonSerializer {
    override fun <T> serialize(value: T, type: TypeRef<T>): ByteArray {
        // Json.encodeToStream takes 3x longer for some reason?
        return jsonFormat.encodeToString(serializer(type), value).toByteArray()
    }

    override fun <T> deserialize(json: ByteArray, type: TypeRef<T>): T {
        // Json.decodeFromStream takes 3x longer for some reason?
        return jsonFormat.decodeFromString(serializer(type), String(json))
    }

    private fun <T> serializer(typeRef: TypeRef<T>): KSerializer<T> {
        // Ideally we'd capture the Kotlin KType (or, more likely, the KSerializer) when creating the
        // TypeRef, but as of Kotlin 1.7 that's very slow, to the extent that it dominates
        // serialization time. See https://github.com/Kotlin/kotlinx.serialization/issues/1348
        //
        // As a compromise, create the KSerializer from the Java Type, and cache it.
        // This is a partial solution that works well for non-nullable types.
        @Suppress("UNCHECKED_CAST")
        return serializerCache.getOrPut(typeRef.type) { serializer(typeRef.type) } as KSerializer<T>
    }
}
