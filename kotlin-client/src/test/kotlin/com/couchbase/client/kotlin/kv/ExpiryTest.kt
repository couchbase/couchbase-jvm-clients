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

import com.couchbase.client.kotlin.kv.Expiry.Companion.of
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.greaterThanOrEqualTo
import org.hamcrest.Matchers.lessThanOrEqualTo
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.TimeUnit.DAYS
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes

internal class ExpiryTest {

    @Test
    fun `none encodes to zero`() {
        assertEquals(0, Expiry.none().encode())
    }

    @Test
    fun `zero duration is invalid`() {
        assertThrows<IllegalArgumentException> { Expiry.of(Duration.ZERO) }
    }

    @Test
    fun `negative duration is invalid`() {
        assertThrows<IllegalArgumentException> { Expiry.ofSeconds(-1) }
    }

    @Test
    fun `duration ending after 2106 is invalid`() {
        val daysUntil2107 = ChronoUnit.DAYS.between(Instant.now(), Instant.parse("2107-01-01T00:00:00Z"))
        assertThrows<IllegalArgumentException> { Expiry.ofDays(daysUntil2107) }
    }

    @Test
    fun `short durations are encoded verbatim`() {
        val longestVerbatimSeconds = DAYS.toSeconds(30) - 1
        assertEquals(longestVerbatimSeconds, Expiry.ofSeconds(longestVerbatimSeconds).encode())
    }

    @Test
    fun `long durations are converted to absolute`() {
        val lowerBound = Instant.now().epochSecond + DAYS.toSeconds(30)
        val actual = Expiry.ofDays(30).encode()
        val upperBound = Instant.now().epochSecond + DAYS.toSeconds(30)

        assertThat(actual, greaterThanOrEqualTo(lowerBound))
        assertThat(actual, lessThanOrEqualTo(upperBound))
    }

    @Test
    fun `zero instant is invalid`() {
        assertThrows<IllegalArgumentException> { Expiry.of(Instant.EPOCH) }
    }

    @Test
    fun `negative instant is invalid`() {
        assertThrows<IllegalArgumentException> { Expiry.of(Instant.ofEpochSecond(-1)) }
    }

    @Test
    fun `instant in distant past is invalid`() {
        assertThrows<IllegalArgumentException> { Expiry.of(Instant.ofEpochSecond(DAYS.toSeconds(30))) }
    }

    @Test
    fun `instant in distant future is invalid`() {
        assertThrows<IllegalArgumentException> { Expiry.of(Instant.ofEpochSecond(DAYS.toSeconds(356) * 200)) }
    }

    @Test
    fun `instant in recent past is encoded verbatim`() {
        val now = Instant.ofEpochSecond(DAYS.toSeconds(31)).epochSecond
        assertEquals(now, Expiry.of(Instant.ofEpochSecond(now)).encode())
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
        assertEquals(Expiry.ofSeconds(60), of(1.minutes))
        assertEquals(of(60.minutes), Expiry.ofHours(1))
        assertEquals(Expiry.ofHours(24), Expiry.ofDays(1))

        assertNotEquals(Expiry.ofSeconds(61), of(1.minutes))
        assertNotEquals(of(61.minutes), Expiry.ofHours(1))
        assertNotEquals(Expiry.ofHours(25), Expiry.ofDays(1))
    }
}
