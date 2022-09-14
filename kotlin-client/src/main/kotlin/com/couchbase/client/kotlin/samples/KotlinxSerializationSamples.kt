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

package com.couchbase.client.kotlin.samples

import com.couchbase.client.kotlin.Cluster
import com.couchbase.client.kotlin.Collection
import com.couchbase.client.kotlin.codec.Content
import com.couchbase.client.kotlin.codec.KotlinxSerializationJsonSerializer
import com.couchbase.client.kotlin.kv.GetResult
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import kotlinx.serialization.serializer

internal fun configureKotlinxSerializationJsonSerializer() {
    // How to make kotlinx.serialization the default JSON serializer.

    // CAVEAT: This serializer cannot handle nullable types.
    // For nullable types, you must do the serialization yourself
    // as shown in the other sample.
    val cluster = Cluster.connect("127.0.0.1", "Administrator", "password") {
        @OptIn(ExperimentalSerializationApi::class)
        jsonSerializer = KotlinxSerializationJsonSerializer()
    }
}

@Serializable
internal data class SampleData<T>(val x: Int, val y: T)

internal suspend fun manualKotlinxSerialization(collection: Collection) {
    // How to do the serialization yourself. Recommended if you do not want
    // to rely on experimental APIs, or if you must serialize nullable types.

    // Here are some convenient extension functions:

    // Returns a `Content` object which smuggles the pre-serialized
    // JSON past the default transcoder.
    fun <T> KSerializer<T>.encodeToJson(value: T): Content =
        Content.json(Json.encodeToString(this, value))

    fun <T> GetResult.decodeFromJson(serializer: KSerializer<T>): T =
        Json.decodeFromString(serializer, String(content.bytes))

    // The SampleData class used here is defined as:
    //
    //     @Serializable
    //     data class SampleData<T>(val x: Int, val y: T)

    // Depending on the Kotlin version, getting a serializer like this
    // might be expensive. Save time by sharing a single instance for each type.
    // See https://github.com/Kotlin/kotlinx.serialization/issues/1348
    val serializer = serializer<SampleData<String?>>()

    collection.upsert("foo", serializer.encodeToJson(SampleData(1, null)))
    val data = collection.get("foo").decodeFromJson(serializer)
}
