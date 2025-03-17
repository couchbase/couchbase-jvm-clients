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

import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.serializer

/**
 * A JsonSerializer for integrating with `kotlinx.serialization`.
 *
 * @sample com.couchbase.client.kotlin.samples.configureKotlinxSerializationJsonSerializer
 */
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
        @Suppress("UNCHECKED_CAST")
        return jsonFormat.serializersModule.serializer(typeRef.ktype) as KSerializer<T>
    }
}
