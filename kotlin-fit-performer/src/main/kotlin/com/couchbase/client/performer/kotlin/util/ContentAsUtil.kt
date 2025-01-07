/*
 * Copyright 2023 Couchbase, Inc.
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
package com.couchbase.client.performer.kotlin.util

import com.couchbase.client.core.json.Mapper
import com.couchbase.client.kotlin.codec.JsonSerializer
import com.couchbase.client.kotlin.codec.Transcoder
import com.couchbase.client.kotlin.kv.GetResult
import com.couchbase.client.kotlin.query.QueryRow
import com.couchbase.client.kotlin.search.SearchRow
import com.couchbase.client.performer.kotlin.manager.toByteString
import com.couchbase.client.protocol.shared.ContentAs as FitContentAs
import com.couchbase.client.protocol.shared.ContentTypes as FitContentTypes

typealias JsonObject = Map<String, Any?>
typealias JsonArray = List<Any?>

fun FitContentAs.convert(
    asByteArray: () -> ByteArray,
    asString: () -> String,
    asJsonObject: () -> JsonObject,
    asJsonArray: () -> JsonArray,
    asBoolean: () -> Boolean,
    asInteger: () -> Int,
    asFloatingPoint: () -> Double
): FitContentTypes {
    return FitContentTypes.newBuilder().apply {
        when {
            hasAsByteArray() -> contentAsBytes = asByteArray().toByteString()
            hasAsString() -> contentAsString = asString()
            hasAsBoolean() -> contentAsBool = asBoolean()
            hasAsInteger() -> contentAsInt64 = asInteger().toLong()
            hasAsFloatingPoint() -> contentAsDouble = asFloatingPoint()
            hasAsJsonObject() -> contentAsBytes = Mapper.encodeAsBytes(asJsonObject()).toByteString()
            hasAsJsonArray() -> contentAsBytes = Mapper.encodeAsBytes(asJsonArray()).toByteString()
            else -> throw UnsupportedOperationException("Kotlin performer cannot handle contentAs $this")
        }
    }.build()
}

fun FitContentAs.convert(
    result: GetResult,
    transcoder: Transcoder? = null,
): FitContentTypes {
    with(result) {
        return convert(
            { content.bytes },
            { contentAs<String>(transcoder) },
            { contentAs<JsonObject>(transcoder) },
            { contentAs<JsonArray>(transcoder) },
            { contentAs<Boolean>(transcoder) },
            { contentAs<Int>(transcoder) },
            { contentAs<Double>(transcoder) },
        )
    }
}

fun FitContentAs.convert(
    result: QueryRow,
    serializer: JsonSerializer? = null,
) : FitContentTypes {
    with(result) {
        return convert(
            { content },
            { contentAs<String>(serializer) },
            { contentAs<JsonObject>(serializer) },
            { contentAs<JsonArray>(serializer) },
            { contentAs<Boolean>(serializer) },
            { contentAs<Int>(serializer) },
            { contentAs<Double>(serializer) },
        )
    }
}

fun FitContentAs.convert(
    result: SearchRow,
    serializer: JsonSerializer? = null,
) : FitContentTypes {
    with(result) {
        return convert(
            { fields!! },
            { fieldsAs<String>(serializer)!! },
            { fieldsAs<JsonObject>(serializer)!! },
            { fieldsAs<JsonArray>(serializer)!! },
            { fieldsAs<Boolean>(serializer)!! },
            { fieldsAs<Int>(serializer)!! },
            { fieldsAs<Double>(serializer)!! },
        )
    }
}
