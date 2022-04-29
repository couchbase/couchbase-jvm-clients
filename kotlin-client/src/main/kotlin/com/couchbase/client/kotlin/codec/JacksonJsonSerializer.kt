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

import com.fasterxml.jackson.databind.JavaType
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.json.JsonMapper

public class JacksonJsonSerializer(private val mapper: ObjectMapper) : JsonSerializer {

    // For binary compatibility with 1.0.0, which only worked with JsonMapper
    public constructor(mapper: JsonMapper) : this(mapper as ObjectMapper)

    override fun <T> serialize(value: T, type: TypeRef<T>): ByteArray = mapper.writeValueAsBytes(value)

    override fun <T> deserialize(json: ByteArray, type: TypeRef<T>): T {
        val javaType: JavaType = mapper.typeFactory.constructType(type.type)
        val result: T = mapper.readValue(json, javaType)
        if (result == null && !type.nullable) {
            throw NullPointerException("Can't deserialize null value into non-nullable type $type")
        }
        return result
    }
}
