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
import com.couchbase.client.core.api.kv.CoreExpiry.EARLIEST_VALID_EXPIRY_INSTANT
import com.couchbase.client.core.api.kv.CoreExpiry.LATEST_VALID_EXPIRY_INSTANT
import java.time.Instant
import java.util.concurrent.TimeUnit.MILLISECONDS
import kotlin.time.Duration
import kotlin.time.toJavaDuration

public sealed class Expiry {
    internal abstract fun encode(): CoreExpiry

    public object None : Expiry() {
        override fun encode() = CoreExpiry.NONE
        override fun toString(): String = "None"
    }

    /**
     * This is a "null object" that cannot be used to set the expiry of a document.
     *
     * Expiry.Unknown exists so [GetResult.expiry] can always be non-null.
     */
    public object Unknown : Expiry() {
        override fun encode() = throw IllegalArgumentException(
            "Expiry.Unknown cannot be used to set a document's expiry. " +
                    "To get a usable Expiry instance, pass `withExpiry = true` when getting the document.")
        override fun toString(): String = "Unknown (To know expiry, pass `withExpiry = true` when calling `get`)"
    }

    public class Absolute internal constructor(public val instant: Instant) : Expiry() {
        private val core: CoreExpiry = CoreExpiry.of(instant)

        init {
            // CoreExpiry treats zero instant as "none", but Kotlin SDK is stricter and disallows.
            require(instant.epochSecond != 0L) {
                "Expiry instant $instant is too far in the past." +
                        " Earliest valid expiry instant is $EARLIEST_VALID_EXPIRY_INSTANT"
            }
        }

        override fun encode() = core

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as Absolute

            if (instant != other.instant) return false

            return true
        }

        override fun hashCode(): Int {
            return instant.hashCode()
        }

        override fun toString(): String {
            return "Absolute(instant=$instant)"
        }
    }

    public class Relative internal constructor(public val duration: Duration) : Expiry() {
        private val core: CoreExpiry = CoreExpiry.of(duration.toJavaDuration())

        init {
            val seconds = duration.inWholeSeconds

            // CoreExpiry treats zero duration as "none", but Kotlin SDK is stricter and disallows.
            require(seconds > 0L) {
                expiryErrorMessage("expiry duration $duration is less than one second")
            }

            require(currentTimeSeconds() + seconds <= LATEST_VALID_EXPIRY_INSTANT.epochSecond) {
                expiryErrorMessage(
                    "expiry duration $duration ends too far in the future." +
                            " Latest valid expiry instant is $LATEST_VALID_EXPIRY_INSTANT"
                )
            }
        }

        override fun encode(): CoreExpiry = core

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as Relative

            if (duration != other.duration) return false

            return true
        }

        override fun hashCode(): Int {
            return duration.hashCode()
        }

        override fun toString(): String {
            return "Relative(duration=$duration)"
        }
    }

    public companion object {
        /**
         * The document will never expire.
         */
        public fun none(): Expiry = None

        /**
         * The document will expire after the given duration.
         * @throws IllegalArgumentException if the duration is less than 1 second.
         */
        public fun of(duration: Duration): Expiry = if (duration.isInfinite()) none() else Relative(duration)

        /**
         * The document will expire at the given instant.
         * If the instant is in the recent past, the document will expire immediately.
         * @throws IllegalArgumentException if the instant is in the distant past
         * (prior to 1970-02-01T00:00:00Z).
         */
        public fun of(instant: Instant): Expiry = Absolute(instant)
    }
}

private fun currentTimeSeconds() = MILLISECONDS.toSeconds(System.currentTimeMillis())

private fun expiryErrorMessage(reason: String) =
    "${reason}." +
            " If you want to disable expiration, use Expiry.none() instead." +
            " If for some reason you want the document to expire immediately," +
            " use Expiry.absolute(Instant.ofEpochSecond(DAYS.toSeconds(31)))."
