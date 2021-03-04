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

import com.couchbase.client.core.error.DecodingFailureException
import com.couchbase.client.core.error.InvalidArgumentException
import com.couchbase.client.core.msg.kv.CodecFlags
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

internal class RawJsonTranscoderTest {

    private val transcoder: Transcoder = RawJsonTranscoder

    @Test
    fun `echoes pre-encoded content`() {
        val content = Content.serializedJavaObject("xyzzy".toByteArray())
        assertSame(content, transcoder.encode(content))
    }

    @Test
    fun `fails to encode null`() {
        assertThrows<InvalidArgumentException> {
            transcoder.encode<String?>(null)
        }
    }

    @Test
    fun `can encode string`() {
        val serialized = transcoder.encode("xyzzy")
        assertArrayEquals("xyzzy".toByteArray(), serialized.bytes)
        assertEquals(CodecFlags.JSON_COMPAT_FLAGS, serialized.flags)
    }

    @Test
    fun `can decode as string`() {
        assertEquals("xyzzy", transcoder.decode<String>(Content.json("xyzzy")))
    }

    @Test
    fun `can encode bytes`() {
        val serialized = transcoder.encode("xyzzy".toByteArray())
        assertArrayEquals("xyzzy".toByteArray(), serialized.bytes)
        assertEquals(CodecFlags.JSON_COMPAT_FLAGS, serialized.flags)
    }

    @Test
    fun `can decode as bytes`() {
        assertArrayEquals("xyzzy".toByteArray(), transcoder.decode<ByteArray>(Content.json("xyzzy")))
    }

    @Test
    fun `fails to encode list`() {
        assertThrows<InvalidArgumentException> {
            transcoder.encode(Object())
        }
    }

    @Test
    fun `fails to decode as list`() {
        assertThrows<DecodingFailureException> {
            transcoder.decode<List<String>>(Content.json("[]"))
        }
    }
}


