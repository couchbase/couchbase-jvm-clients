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
import com.fasterxml.jackson.databind.json.JsonMapper

/**
 * A JSON serializer backed by Jackson.
 *
 * Requires adding Jackson 2.x as an additional dependency of your project.
 * Maven coordinates of the additional dependency:
 *
 * ```
 * <dependency>
 *     <groupId>com.fasterxml.jackson.module</groupId>
 *     <artifactId>jackson-module-kotlin</artifactId>
 *     <version>${jackson.version}</version>
 * </dependency>
 * ```
 *
 * Sample code for configuring the Couchbase cluster environment:
 *
 *```
 * val cluster = Cluster.connect("localhost", "Administrator", "password") {
 *     jsonSerializer = Jackson2JsonSerializer(jsonMapper { addModule(KotlinModule()) })
 * }
 * ```
 */
public class Jackson2JsonSerializer(private val mapper: JsonMapper) : JsonSerializer {
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
