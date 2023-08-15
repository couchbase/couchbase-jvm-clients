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
import com.couchbase.client.protocol.shared.ContentAs
import com.couchbase.client.protocol.shared.ContentTypes
import com.google.protobuf.ByteString

typealias JsonObject = Map<String, Any?>
typealias JsonArray = List<Any?>

class ContentAsUtil {

    companion object {
        fun contentType(
            contentAs: ContentAs,
            asByteArray: () -> ByteArray,
            asString: () -> String,
            asJsonObject: () -> JsonObject,
            asJsonArray: () -> JsonArray,
            asBoolean: () -> Boolean,
            asInteger: () -> Int,
            asFloatingPoint: () -> Double
        ): ContentTypes {
            return if (contentAs.hasAsString()) {
                ContentTypes.newBuilder().setContentAsString(asString()).build()
            } else if (contentAs.hasAsByteArray()) {
                ContentTypes.newBuilder().setContentAsBytes(ByteString.copyFrom(asByteArray())).build()
            } else if (contentAs.hasAsJsonObject()) {
                val json = asJsonObject()
                val converted = Mapper.encodeAsBytes(json)
                ContentTypes.newBuilder().setContentAsBytes(ByteString.copyFrom(converted)).build()
            } else if (contentAs.hasAsJsonArray()) {
                val json = asJsonArray()
                val converted = Mapper.encodeAsBytes(json)
                ContentTypes.newBuilder().setContentAsBytes(ByteString.copyFrom(converted)).build()
            } else if (contentAs.hasAsBoolean()) {
                ContentTypes.newBuilder().setContentAsBool(asBoolean()).build()
            } else if (contentAs.hasAsInteger()) {
                ContentTypes.newBuilder().setContentAsInt64(asInteger().toLong()).build()
            } else if (contentAs.hasAsFloatingPoint()) {
                ContentTypes.newBuilder().setContentAsDouble(asFloatingPoint()).build()
            } else {
                throw UnsupportedOperationException("Kotlin performer cannot handle contentAs ${contentAs}")
            }
        }
    }
}
