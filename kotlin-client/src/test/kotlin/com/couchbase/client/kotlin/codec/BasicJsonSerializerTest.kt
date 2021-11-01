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

import com.couchbase.client.kotlin.internal.toStringUtf8
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

internal class BasicJsonSerializerTest {
    private val serializer = BasicJsonSerializer()

    @Test
    fun `can serialize null`() {
        assertArrayEquals("null".toByteArray(), serializer.serialize(null as String?))
    }

    @Test
    fun `can deserialize null`() {
        assertNull(serializer.deserialize<String?>("null".toByteArray()))
    }

    @Test
    fun `prevents null from sneaking into non-nullable type`() {
        assertThrows<NullPointerException> {
            serializer.deserialize<String>("null".toByteArray())
        }
    }

    @Test
    fun `list of primitives survives round trip`() {
        val testSubject = listOf("a", 1, true)

        val encoded = serializer.serialize(testSubject)
        assertEquals("[\"a\",1,true]", encoded.toStringUtf8())

        assertEquals(testSubject, serializer.deserialize(encoded))
    }

    @Test
    fun `set of primitives survives round trip`() {
        val testSubject = setOf("a", 1, true)

        val encoded = serializer.serialize(testSubject)
        assertEquals("[\"a\",1,true]", encoded.toStringUtf8())

        val result: Set<Any?> = serializer.deserialize(encoded)
        assertEquals(testSubject, result)
    }

    @Test
    fun `map of primitives survives round trip`() {
        val testSubject = mapOf("foo" to listOf("a", 1, true))

        val encoded = serializer.serialize(testSubject)
        assertEquals("{\"foo\":[\"a\",1,true]}", encoded.toStringUtf8())

        run {
            val result: Map<String, List<*>> = serializer.deserialize(encoded)
            assertEquals(testSubject, result)
        }

        run {
            val result: Map<String, List<Any?>> = serializer.deserialize(encoded)
            assertEquals(testSubject, result)
        }
    }

    data class SerializeMe(val magicWord: String)

    @Test
    fun `prevents serializing pojos`() {
        assertThrows<IllegalArgumentException> {
            serializer.serialize(SerializeMe("alakazam"))
        }

        assertThrows<IllegalArgumentException> {
            serializer.serialize(listOf(SerializeMe("alakazam"), SerializeMe("abracadabra")))
        }

        assertThrows<IllegalArgumentException> {
            serializer.serialize(mapOf(SerializeMe("alakazam") to "foo"))
        }
    }


    @Test
    fun `prevents deserializing pojos`() {
        val encoded = "null".toByteArray()

        assertThrows<IllegalArgumentException> {
            serializer.deserialize<SerializeMe>(encoded)
        }

        assertThrows<IllegalArgumentException> {
            serializer.deserialize<List<SerializeMe>>(encoded)
        }

        assertThrows<IllegalArgumentException> {
            serializer.deserialize<Set<SerializeMe>>(encoded)
        }

        assertThrows<IllegalArgumentException> {
            serializer.deserialize<Map<String, List<SerializeMe>>>(encoded)
        }

        assertThrows<IllegalArgumentException> {
            serializer.deserialize<Map<SerializeMe, String>>(encoded)
        }
    }
}
