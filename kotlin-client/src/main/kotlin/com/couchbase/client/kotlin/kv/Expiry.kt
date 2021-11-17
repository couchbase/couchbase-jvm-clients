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

import java.time.Instant
import java.util.concurrent.TimeUnit.DAYS
import java.util.concurrent.TimeUnit.MILLISECONDS
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

public sealed class Expiry {
    internal abstract fun encode(): Long

    public object None : Expiry() {
        override fun encode() = 0L
        override fun toString(): String = "None"
    }

    public data class Absolute internal constructor(public val instant: Instant) : Expiry() {
        init {
            // If we were to require the instant be in the future, it could cause problems
            // when creating the Absolute object for a GetResult with expiry.
            // Also, it must be possible to preserve the existing expiry time when
            // updating the document, even if it means the document expires immediately.
            // We can, however, do a basic sanity check.
            require(instant >= EARLIEST_VALID_EXPIRY_INSTANT) {
                expiryErrorMessage(
                    "expiry instant $instant is in the distant past." +
                            " Earliest valid expiry instant is $EARLIEST_VALID_EXPIRY_INSTANT"
                )
            }

            require(instant <= LATEST_VALID_EXPIRY_INSTANT) {
                expiryErrorMessage(
                    "expiry instant $instant is too far in the future." +
                            " Latest valid expiry instant is $LATEST_VALID_EXPIRY_INSTANT"
                )
            }
        }

        override fun encode() = instant.epochSecond
    }

    public data class Relative internal constructor(val duration: Duration) : Expiry() {
        init {
            val seconds = duration.inWholeSeconds
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

        override fun encode(): Long {
            val seconds: Long = duration.inWholeSeconds

            // If it's under the threshold, let the server convert it to an absolute time.
            // Otherwise we need to do the conversion on the client.
            return if (seconds < RELATIVE_EXPIRY_CUTOFF_SECONDS) seconds
            else currentTimeSeconds() + seconds
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
         * The document will expire after the given number of seconds.
         * Shorthand for `Expiry.of(x.seconds)`
         * @throws IllegalArgumentException if seconds < 1
         */
        @Deprecated(
            message = "Deprecated in favor of Expiry.of(x.seconds)",
            replaceWith = ReplaceWith(
                expression = "of(seconds.seconds)",
                imports = ["kotlin.time.Duration.Companion.seconds"]
            ),
        )
        public fun ofSeconds(seconds: Long): Expiry = of(seconds.seconds)

        /**
         * The document will expire after the given number of minutes.
         * Shorthand for `Expiry.of(x.minutes)`
         * @throws IllegalArgumentException if minutes < 1
         */
        @Deprecated(
            message = "Deprecated in favor of Expiry.of(x.minutes)",
            replaceWith = ReplaceWith(
                expression = "of(minutes.minutes)",
                imports = ["kotlin.time.Duration.Companion.minutes"]
            ),
        )
        public fun ofMinutes(minutes: Long): Expiry = of(minutes.minutes)

        /**
         * The document will expire after the given number of hours.
         * Shorthand for `Expiry.of(Duration.ofHours(x))`
         * @throws IllegalArgumentException if hours < 1
         */
        @Deprecated(
            message = "Deprecated in favor of Expiry.of(x.hours)",
            replaceWith = ReplaceWith(
                expression = "of(hours.hours)",
                imports = ["kotlin.time.Duration.Companion.hours"]
            ),
        )
        public fun ofHours(hours: Long): Expiry = of(hours.hours)

        /**
         * The document will expire after the given number of days.
         * Shorthand for `Expiry.of(x.days)`
         * @throws IllegalArgumentException if days < 1
         */
        @Deprecated(
            message = "Deprecated in favor of Expiry.of(x.days)",
            replaceWith = ReplaceWith(
                expression = "of(days.days)",
                imports = ["kotlin.time.Duration.Companion.minutes"]
            ),
        )
        public fun ofDays(days: Long): Expiry = of(days.days)

        /**
         * The document will expire at the given instant.
         * If the instant is in the recent past, the document will expire immediately.
         * @throws IllegalArgumentException if the instant is in the distant past
         * (prior to 1970-02-01T00:00:00Z).
         */
        public fun of(instant: Instant): Expiry = Absolute(instant)
    }
}

// Durations longer than this must be converted to an absolute
// epoch second before being passed to the server.
private val RELATIVE_EXPIRY_CUTOFF_SECONDS = DAYS.toSeconds(30).toInt()

// Any instant earlier than this is almost certainly the result
// of a programming error. The selected value is > 30 days so
// we don't need to worry about the relative expiry cutoff.
private val EARLIEST_VALID_EXPIRY_INSTANT = Instant.ofEpochSecond(DAYS.toSeconds(31))

// The server interprets the 32-bit expiry field as an unsigned
// integer. This means the maximum value is 4294967295 seconds,
// which corresponds to 2106-02-07T06:28:15Z.
private val LATEST_VALID_EXPIRY_INSTANT = Instant.ofEpochSecond(4294967295)

private fun currentTimeSeconds() = MILLISECONDS.toSeconds(System.currentTimeMillis())

private fun expiryErrorMessage(reason: String) =
    "Document would expire immediately; ${reason}." +
            " If you want to disable expiration, use Expiry.none() instead." +
            " If for some reason you want the document to expire immediately," +
            " use Expiry.absolute(Instant.ofEpochSecond(DAYS.toSeconds(31)))."
