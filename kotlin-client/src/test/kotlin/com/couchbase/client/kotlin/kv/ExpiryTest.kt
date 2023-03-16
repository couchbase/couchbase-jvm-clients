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

package com.couchbase.client.kotlin.kv

import com.couchbase.client.core.api.kv.CoreExpiry
import com.couchbase.client.core.error.InvalidArgumentException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit.DAYS
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

internal class ExpiryTest {

    @Test
    fun `none encodes to core none`() {
        assertEquals(CoreExpiry.NONE, Expiry.none().encode())
    }

    @Test
    fun `zero duration is invalid`() {
        assertThrows<IllegalArgumentException> { Expiry.of(Duration.ZERO) }
    }

    @Test
    fun `negative duration is invalid`() {
        assertThrows<InvalidArgumentException> { Expiry.of((-1).seconds) }
    }

    @Test
    fun `duration ending after 2106 is invalid`() {
        val daysUntil2107 = ChronoUnit.DAYS.between(Instant.now(), Instant.parse("2107-01-01T00:00:00Z"))
        assertThrows<IllegalArgumentException> { Expiry.of(daysUntil2107.days) }
    }

    @Test
    fun `zero instant is invalid`() {
        assertThrows<IllegalArgumentException> { Expiry.of(Instant.EPOCH) }
    }

    @Test
    fun `negative instant is invalid`() {
        assertThrows<InvalidArgumentException> { Expiry.of(Instant.ofEpochSecond(-1)) }
    }

    @Test
    fun `instant in distant past is invalid`() {
        assertThrows<InvalidArgumentException> { Expiry.of(Instant.ofEpochSecond(DAYS.toSeconds(30))) }
    }

    @Test
    fun `instant in distant future is invalid`() {
        assertThrows<InvalidArgumentException> { Expiry.of(Instant.ofEpochSecond(DAYS.toSeconds(356) * 200)) }
    }

    @Test
    fun `absolute are equal if they have the same epoch second`() {
        val now = Instant.parse("2020-01-01T00:00:00Z").epochSecond
        assertEquals(
            Expiry.of(Instant.ofEpochSecond(now)),
            Expiry.of(Instant.ofEpochSecond(now))
        )
        assertNotEquals(
            Expiry.of(Instant.ofEpochSecond(now)),
            Expiry.of(Instant.ofEpochSecond(now + 1))
        )
    }

    @Test
    fun `relative are equal if they have the same duration`() {
        assertEquals(Expiry.of(60.seconds), Expiry.of(1.minutes))
        assertEquals(Expiry.of(60.minutes), Expiry.of(1.hours))
        assertEquals(Expiry.of(24.hours), Expiry.of(1.days))

        assertNotEquals(Expiry.of(61.seconds), Expiry.of(1.minutes))
        assertNotEquals(Expiry.of(61.minutes), Expiry.of(1.hours))
        assertNotEquals(Expiry.of(25.hours), Expiry.of(1.days))
    }
}
