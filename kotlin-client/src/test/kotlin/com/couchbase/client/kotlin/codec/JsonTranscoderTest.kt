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

import com.couchbase.client.core.error.InvalidArgumentException
import com.couchbase.client.core.msg.kv.CodecFlags
import com.couchbase.client.kotlin.internal.toStringUtf8
import com.fasterxml.jackson.module.kotlin.jsonMapper
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

internal class JsonTranscoderTest {
    private val transcoder: Transcoder = JsonTranscoder(JacksonJsonSerializer(jsonMapper()))

    @Test
    fun `echoes pre-encoded content`() {
        val content = Content.serializedJavaObject("xyzzy".toByteArray())
        assertSame(content, transcoder.encode(content))
    }

    @Test
    fun `null survives round trip`() {
        val testSubject = null as String?

        val encoded = transcoder.encode(testSubject)
        assertEquals(CodecFlags.JSON_COMPAT_FLAGS, encoded.flags)

        val decoded = transcoder.decode<Any?>(encoded)
        assertEquals(testSubject, decoded)
    }

    @Test
    fun `prevents null from sneaking into non-nullable type`() {
        val encoded = transcoder.encode(null as String?)
        assertThrows(NullPointerException::class.java) {
            transcoder.decode<Any>(encoded)
        }
    }

    @Test
    fun `serializable content survives round trip`() {
        val testSubject = listOf("a", 1, true)

        val encoded = transcoder.encode(testSubject)
        assertEquals(CodecFlags.JSON_COMPAT_FLAGS, encoded.flags)

        val decoded = transcoder.decode<Any>(encoded)
        assertEquals(testSubject, decoded)
    }

    @Test
    fun `encoded content is json`() {
        val encoded = transcoder.encode(listOf("a", 1, true))
        assertEquals("[\"a\",1,true]", encoded.bytes.toStringUtf8())
    }

    @Test
    fun `fails to decode as bytes`() {
        assertThrows(InvalidArgumentException::class.java) {
            transcoder.decode<ByteArray>(Content.json("\"\""))
        }
    }

    @Test
    fun `fails to encode bytes`() {
        assertThrows(InvalidArgumentException::class.java) {
            transcoder.encode("xyzzy".toByteArray())
        }
    }

}
