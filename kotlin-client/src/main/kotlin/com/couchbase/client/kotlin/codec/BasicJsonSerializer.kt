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

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JavaType
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.json.JsonMapper
import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type
import java.lang.reflect.WildcardType

/**
 * The default JSON serializer. Does not depend on any external libraries,
 * but has limited capabilities.
 *
 * It can serialize and deserialize values of type Int, Long, Double,
 * Boolean, String, Map, List, and Set.
 *
 * If you want to do data binding (converting JSON directly to custom Kotlin
 * objects and vice versa), please configure your cluster environment to use a more
 * capable serializer such as [Jackson2JsonSerializer] or [MoshiJsonSerializer].
 * See the documentation of those classes for more details.
 */
public class BasicJsonSerializer : JsonSerializer {
    private val mapper = JsonMapper()

    override fun <T> serialize(value: T, type: TypeRef<T>): ByteArray {
        checkValue(value)
        return mapper.writeValueAsBytes(value)
    }

    private fun <T> checkValue(value: T) {
        when (value) {
            null -> return
            is Int -> return
            is Long -> return
            is Double -> return
            is Boolean -> return
            is String -> return
            is List<*> -> return value.forEach { checkValue(it) }
            is Set<*> -> return value.forEach { checkValue(it) }
            is Map<*, *> -> {
                value.forEach { (k, v) ->
                    require(k is String) {
                        "${javaClass.simpleName} can only handle Maps whose keys are String, but found ${k?.javaClass}. " +
                                "For advanced functionality, please configure a different JSON serializer."
                    }
                    checkValue(v)
                }
                return
            }
            else -> throw IllegalArgumentException(
                "${javaClass.simpleName} can only handle values of type String, Boolean, Int, Long, Double, Map, List, Set, or null, but found ${value.javaClass}. " +
                        "For advanced functionality, please configure a different JSON serializer."
            )
        }

    }

    override fun <T> deserialize(json: ByteArray, type: TypeRef<T>): T {
        require(isSupportedType(type.type)) {
            "${javaClass.simpleName} can only handle values of type String, Boolean, Int, Long, Double, Map, List, Set, or null, but found ${type}. " +
                    "For advanced functionality, please configure a different JSON serializer."
        }

        val javaType: JavaType = mapper.typeFactory.constructType(type.type)
        val result: T = mapper.readValue(json, javaType)
        if (result == null && !type.nullable) {
            throw NullPointerException("Can't deserialize null value into non-nullable type $type")
        }
        return result
    }
}

private val supportedPrimitiveTypes = setOf(
    java.lang.Object::class.java,
    java.lang.String::class.java,
    java.lang.Boolean::class.java,
    java.lang.Integer::class.java,
    java.lang.Long::class.java,
    java.lang.Double::class.java,
)

private val supportedCollectionTypes = setOf(
    java.util.List::class.java,
    java.util.Set::class.java,
)

private fun isSupportedType(type: Type): Boolean {
    return when (type) {
        in supportedPrimitiveTypes -> true

        is WildcardType -> type.lowerBounds.all { isSupportedType(it) }
                && type.upperBounds.all { isSupportedType(it) }

        is ParameterizedType -> {
            if (type.rawType == java.util.Map::class.java) {
                return (type.actualTypeArguments[0] == String::class.java || type.actualTypeArguments[0] is WildcardType)
                        && isSupportedType(type.actualTypeArguments[1])
            }

            return type.rawType in supportedCollectionTypes
                    && isSupportedType(type.actualTypeArguments[0])
        }

        else -> false
    }
}
