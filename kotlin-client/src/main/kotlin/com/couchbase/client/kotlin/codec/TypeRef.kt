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

import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type

/**
 * A reified Java type. Conveys generic type information at run time.
 *
 * Create new instances with the [typeRef] factory method:
 *
 * ```
 * val listOfStrings = typeRef<List<String>>()
 * ```
 *
 * Uses the technique described in Neal Gafter's article on
 * [Super Type Tokens](http://gafter.blogspot.com/2006/12/super-type-tokens.html).
 *
 * @param <T> The type to represent
 */
public abstract class TypeRef<T> protected constructor(
    public val nullable: Boolean,
) {
    public val type: Type = (javaClass.genericSuperclass as ParameterizedType)
        .actualTypeArguments[0]

    override fun toString(): String = type.typeName + if (nullable) "?" else ""
}

/**
 * Returns a reified version of the type parameter.
 */
public inline fun <reified T> typeRef(): TypeRef<T> {
    return object : TypeRef<T>(nullable = null is T) {}
}
