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

package com.couchbase.client.kotlin

import com.couchbase.client.core.error.DocumentNotFoundException
import com.couchbase.client.kotlin.kv.Expiry
import com.couchbase.client.kotlin.util.KotlinIntegrationTest
import com.couchbase.client.test.ClusterType.MOCKED
import com.couchbase.client.test.IgnoreWhen
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Duration
import java.time.Duration.ofDays
import java.time.Instant
import java.time.temporal.ChronoUnit.DAYS
import java.time.temporal.ChronoUnit.SECONDS
import java.util.*

@IgnoreWhen(clusterTypes = [MOCKED])
internal class ExpiryIntegrationTest : KotlinIntegrationTest() {

    // The server interprets the 32-bit expiry field as an unsigned
    // integer. This means the maximum value is 4294967295 seconds,
    // which corresponds to 2106-02-07T06:28:15Z.
    private val endOfTime = Instant.parse("2106-02-07T06:28:15Z")

    @Test
    fun `document expires immediately when expiry is in past`(): Unit = runBlocking {
        val id = UUID.randomUUID().toString()
        collection.upsert(id, "foo", expiry = Expiry.absolute(Instant.now().minus(3, DAYS)))
        assertThrows<DocumentNotFoundException> { collection.get(id) }
    }

    @Test
    fun `can upsert with absolute expiry in near future`(): Unit = runBlocking {
        checkAbsoluteExpiry(Instant.now().plus(3, DAYS))
    }

    @Test
    fun `can upsert without expiry`(): Unit = runBlocking {
        val id = UUID.randomUUID().toString()
        collection.upsert(id, "foo")
        assertEquals(Expiry.none(), collection.get(id, withExpiry = true).expiry)
    }

    @Test
    fun `GetResult expiry is null when unknown`(): Unit = runBlocking {
        val id = UUID.randomUUID().toString()
        collection.upsert(id, "foo", expiry = Expiry.relative(ofDays(1)))
        assertNull(collection.get(id, withExpiry = false).expiry)
    }

    @Test
    fun `can upsert with short relative expiry`(): Unit = checkRelativeExpiryOfDays(15)

    @Test
    fun `can upsert with long relative expiry`(): Unit = checkRelativeExpiryOfDays(31)

    @Test
    fun `can upsert with absolute expiry beyond 2038`() {
        checkAbsoluteExpiry(endOfTime)
    }

    @Test
    fun `can upsert with relative expiry beyond 2038`() {
        val expiryDays = DAYS.between(Instant.now(), endOfTime)
        checkRelativeExpiryOfDays(expiryDays)
    }

    private fun checkAbsoluteExpiry(instant: Instant): Unit = runBlocking {
        val id = UUID.randomUUID().toString()

        val expiry = Expiry.absolute(instant.truncatedTo(SECONDS))
        collection.upsert(id, "foo", expiry = expiry)

        assertEquals(expiry, collection.get(id, withExpiry = true).expiry)
    }

    private fun checkRelativeExpiryOfDays(days: Long): Unit = runBlocking {
        val id = UUID.randomUUID().toString()

        val duration = Duration.ofDays(days)
        collection.upsert(id, "foo", expiry = Expiry.relative(duration))

        val actualExpiry = collection.get(id, withExpiry = true).expiry as Expiry.Absolute
        assertThat(actualExpiry.instant).isBetween(
            Instant.now().plus(days - 1, DAYS),
            Instant.now().plus(days + 1, DAYS),
        )
    }

}
