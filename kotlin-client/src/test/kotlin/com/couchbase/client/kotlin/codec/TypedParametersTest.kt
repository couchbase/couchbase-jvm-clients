/*
 * Copyright 2025 Couchbase, Inc.
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

package com.couchbase.client.kotlin.codec

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

internal class TypedParametersTest {

    private data class MyDataClass(val value: String)

    @Test
    fun `can set various positional parameter types`() {
        val typedParams = PositionalParametersBuilder().apply {
            param(null as String?)
            param("hello")
            param(true)
            param(1)
            param(2L)
            param(MyDataClass("hello"))
        }.build()

        assertEquals(
            listOf(
                null,
                "hello",
                true,
                1,
                2L,
                MyDataClass("hello")
            ),
            typedParams.map { it.value }
        )

        assertEquals(
            listOf(
                typeRef<String?>(),
                typeRef<String>(),
                typeRef<Boolean>(),
                typeRef<Int>(),
                typeRef<Long>(),
                typeRef<MyDataClass>()
            ).map { it.type },
            typedParams.map { it.type.type }
        )
    }

    @Test
    fun `can set various named parameter types`() {
        val typedParams = NamedParametersBuilder().apply {
            param("nothing", null as String?)
            param("string", "hello")
            param("boolean", true)
            param("int", 1)
            param("long", 2L)
            param("other", MyDataClass("hello"))
        }.build()

        assertEquals(
            mapOf(
                "nothing" to null,
                "string" to "hello",
                "boolean" to true,
                "int" to 1,
                "long" to 2L,
                "other" to MyDataClass("hello")
            ),
            typedParams.mapValues { it.value.value }
        )

        assertEquals(
            mapOf(
                "nothing" to typeRef<String?>().type,
                "string" to typeRef<String>().type,
                "boolean" to typeRef<Boolean>().type,
                "int" to typeRef<Int>().type,
                "long" to typeRef<Long>().type,
                "other" to typeRef<MyDataClass>().type
            ),
            typedParams.mapValues { it.value.type.type }
        )
    }

    @Test
    fun `can clobber named parameter`() {
        val typedParams = NamedParametersBuilder().apply {
            param("foo", 3)
            param("foo", "xyzzy")
        }.build()

        assertEquals(
            setOf("foo"),
            typedParams.keys,
        )

        assertEquals(
            "xyzzy",
            typedParams.values.single().value
        )

        assertEquals(
            typeRef<String>().type,
            typedParams.values.single().type.type
        )
    }
}
