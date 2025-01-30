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
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonPrimitive
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

@OptIn(ExperimentalSerializationApi::class)
internal class KotlinxSerializationJsonSerializerTest {
    private val serializer = KotlinxSerializationJsonSerializer()

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
        assertThrows<SerializationException> {
            serializer.deserialize<String>("null".toByteArray())
        }
    }

    @Test
    fun `list of JsonElements survives round trip`() {
        val testSubject: List<JsonElement> = listOf(JsonPrimitive("a"), JsonPrimitive(1), JsonPrimitive(true))

        val encoded = serializer.serialize(testSubject)
        assertEquals("""["a",1,true]""", encoded.toStringUtf8())

        assertEquals(testSubject, serializer.deserialize<List<JsonElement>>(encoded))
    }

    @Serializable
    data class SerializeMe(val magicWord: String?)

    @Serializable
    data class SerializeMeNonNullProperty(val magicWord: String)

    @Test
    fun `list of serializable objects survives round trip`() {
        val testSubject: List<SerializeMe> = listOf(SerializeMe("alakazam"), SerializeMe("abracadabra"))

        val encoded = serializer.serialize(testSubject)
        assertEquals("""[{"magicWord":"alakazam"},{"magicWord":"abracadabra"}]""", encoded.toStringUtf8())

        assertEquals(testSubject, serializer.deserialize<List<SerializeMe>>(encoded))
    }

    @Test
    fun `object property can be nullable`() {
        val testSubject = SerializeMe(null)

        val encoded = serializer.serialize(testSubject)
        assertEquals("""{"magicWord":null}""", encoded.toStringUtf8())

        assertEquals(testSubject, serializer.deserialize<SerializeMe>(encoded))
    }

    @Test
    fun `throws if non-nullable property is null`() {
        assertThrows<SerializationException> {
            val encoded = """{"magicWord":null}"""
            serializer.deserialize<SerializeMeNonNullProperty>(encoded.toByteArray(), typeRef())
        }
    }

    @Serializable
    data class SerializeMeDifferently(@SerialName("x") val magicWord: String)

    @Test
    fun `respects SerialName annotation`() {
        val testSubject = SerializeMeDifferently("alakazam")

        val encoded = serializer.serialize(testSubject)
        assertEquals("""{"x":"alakazam"}""", encoded.toStringUtf8())

        assertEquals(testSubject, serializer.deserialize<SerializeMeDifferently>(encoded))
    }

    @Test
    fun `user can configure json parsing options`() {
        val jsonWithUnknownKey = """{"magicWord":"alakazam","bogusProperty":123}""".toByteArray()

        assertThrows<SerializationException> {
            serializer.deserialize(jsonWithUnknownKey, typeRef<SerializeMe>())
        }

        val customSerializer = KotlinxSerializationJsonSerializer(Json { ignoreUnknownKeys = true })
        val deserialized = customSerializer.deserialize(jsonWithUnknownKey, typeRef<SerializeMe>())
        assertEquals(SerializeMe("alakazam"), deserialized)
    }
}
