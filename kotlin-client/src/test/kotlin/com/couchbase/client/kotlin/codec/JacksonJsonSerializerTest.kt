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
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.jsonMapper
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

internal class JacksonJsonSerializerTest {
    private val serializer = JacksonJsonSerializer(jsonMapper { addModule(KotlinModule()) })

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

    data class SerializeMe(val magicWord: String)

    @Test
    fun `list of pojos survives round trip`() {
        val testSubject: List<SerializeMe> = listOf(SerializeMe("alakazam"), SerializeMe("abracadabra"))

        val encoded = serializer.serialize(testSubject)
        assertEquals("[{\"magicWord\":\"alakazam\"},{\"magicWord\":\"abracadabra\"}]", encoded.toStringUtf8())

        assertEquals(testSubject, serializer.deserialize<List<SerializeMe>>(encoded))
    }

    data class SerializeMeDifferently(@JsonProperty("x") val magicWord: String)

    @Test
    fun `respects JsonProperty annotation`() {
        val testSubject = SerializeMeDifferently("alakazam")

        val encoded = serializer.serialize(testSubject)
        assertEquals("{\"x\":\"alakazam\"}", encoded.toStringUtf8())

        assertEquals(testSubject, serializer.deserialize<SerializeMeDifferently>(encoded))
    }

    @Test
    fun `can create from ObjectMapper`() {
        JacksonJsonSerializer(ObjectMapper())
    }
}
