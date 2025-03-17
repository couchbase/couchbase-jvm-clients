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
import kotlin.reflect.KType
import kotlin.reflect.typeOf

/**
 * A reified type. Conveys generic type information at run time.
 *
 * Create new instances with the [typeRef] factory method:
 *
 * ```
 * val listOfStrings = typeRef<List<String>>()
 * ```
 *
 * Implementation Note:* The KType returned by [typeOf] includes an unannotated version of the Java type.
 * To ensure important annotations are available to Java data binding libraries,
 * we capture the Java type separately using the technique described in Neal Gafter's article on
 * [Super Type Tokens](http://gafter.blogspot.com/2006/12/super-type-tokens.html).
 *
 * @param <T> The type to represent
 */
public abstract class TypeRef<T> protected constructor(
    public val nullable: Boolean,
    public val ktype: KType,
) {
    @Deprecated(level = DeprecationLevel.HIDDEN, message = "For binary compatibility pre 1.5.0")
    protected constructor(nullable: Boolean) : this(nullable, typeOf<KTypeNotAvailable_PleaseRecompileProject>())

    public val type: Type = (javaClass.genericSuperclass as ParameterizedType)
        .actualTypeArguments[0]

    override fun toString(): String = ktype.toString()
}

/**
 * Returns a reified version of the type parameter.
 */
public inline fun <reified T> typeRef(): TypeRef<T> {
    // The typeOf() intrinsic is still relatively expensive in Kotlin 2.1.
    // Getting the KType adds ~2x to ~8x additional overhead compared to getting just the Java type,
    // depending on whether the type is simple (like `Foo`) or parameterized (like `List<Foo>`).
    // This is a small fraction of total serialization time, but not unmeasurable.
    // Avoid this cost if the user provides pre-serialized Content, because that's the fast path
    // for users who care enough about serialization performance to optimize it themselves.
    if (T::class == Content::class) {
        @Suppress("UNCHECKED_CAST")
        return contentTypeRef as TypeRef<T>
    }

    return object : TypeRef<T>(nullable = null is T, typeOf<T>()) {}
}

@PublishedApi
internal val contentTypeRef: TypeRef<Content> = object : TypeRef<Content>(nullable = false, typeOf<Content>()) {}

/**
 * Prior to Couchbase Kotlin SDK 1.5.0, the inline [typeRef] function did not capture
 * the [KType] required by 1.5.0's stable version of [KotlinxSerializationJsonSerializer].
 * If you're reading this comment because you got an error, recompiling your project will likely fix it.
 */
private class KTypeNotAvailable_PleaseRecompileProject
