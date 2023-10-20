/*
 * Copyright 2022 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.kotlin.util

import com.couchbase.client.kotlin.util.StorageSizeUnit.BYTES
import com.couchbase.client.kotlin.util.StorageSizeUnit.GIBIBYTES
import com.couchbase.client.kotlin.util.StorageSizeUnit.KIBIBYTES
import com.couchbase.client.kotlin.util.StorageSizeUnit.MEBIBYTES
import com.couchbase.client.kotlin.util.StorageSizeUnit.PEBIBYTES
import com.couchbase.client.kotlin.util.StorageSizeUnit.TEBIBYTES

public enum class StorageSizeUnit(
    internal val shortName: String,
    internal val scale: Long,
) {
    BYTES("B", 1L),
    KIBIBYTES("KiB", 1024L),
    MEBIBYTES("MiB", 1024L * 1024),
    GIBIBYTES("GiB", 1024L * 1024 * 1024),
    TEBIBYTES("TiB", 1024L * 1024 * 1024 * 1024),
    PEBIBYTES("PiB", 1024L * 1024 * 1024 * 1024 * 1024),
    ;

    override fun toString(): String = shortName
}

/**
 * A size measured in bytes, kibibytes, mebibytes, etc.
 *
 * Example:
 * ```
 * val quota: StorageSize = 256.mebibytes
 * ```
 */
public class StorageSize(
    public val value: Long,
    public val unit: StorageSizeUnit,
) : Comparable<StorageSize> {
    public val inBytes: Long

    init {
        require(value >= 0) { "Storage size value must not be negative, but got: $value ${unit.shortName}" }

        try {
            this.inBytes = Math.multiplyExact(value, unit.scale)
        } catch (_: ArithmeticException) {
            throw IllegalArgumentException("$value ${unit.shortName} is more than the max value, ${Long.MAX_VALUE} bytes.")
        }
    }

    public companion object {
        public inline val Int.bytes: StorageSize get() = this.toLong().bytes
        public inline val Int.kibibytes: StorageSize get() = this.toLong().kibibytes
        public inline val Int.mebibytes: StorageSize get() = this.toLong().mebibytes
        public inline val Int.gibibytes: StorageSize get() = this.toLong().gibibytes
        public inline val Int.tebibytes: StorageSize get() = this.toLong().tebibytes
        public inline val Int.pebibytes: StorageSize get() = this.toLong().pebibytes

        public inline val Long.bytes: StorageSize get() = StorageSize(this, BYTES)
        public inline val Long.kibibytes: StorageSize get() = StorageSize(this, KIBIBYTES)
        public inline val Long.mebibytes: StorageSize get() = StorageSize(this, MEBIBYTES)
        public inline val Long.gibibytes: StorageSize get() = StorageSize(this, GIBIBYTES)
        public inline val Long.tebibytes: StorageSize get() = StorageSize(this, TEBIBYTES)
        public inline val Long.pebibytes: StorageSize get() = StorageSize(this, PEBIBYTES)
    }

    internal val inWholeKibibytes: Long get() = toLong(KIBIBYTES)
    internal val inWholeMebibytes: Long get() = toLong(MEBIBYTES)
    internal val inWholeGibibytes: Long get() = toLong(GIBIBYTES)
    internal val inWholeTebibytes: Long get() = toLong(TEBIBYTES)
    internal val inWholePebibytes: Long get() = toLong(PEBIBYTES)

    private fun toLong(destUnit: StorageSizeUnit): Long = inBytes / destUnit.scale

    internal fun serialize(): String = "$value ${unit.shortName}"

    internal fun simplify(): StorageSize {
        if (inBytes == 0L) return 0.bytes
        if (value % 1024 != 0L) return this
        val nextUnit = when (unit) {
            BYTES -> KIBIBYTES
            KIBIBYTES -> MEBIBYTES
            MEBIBYTES -> GIBIBYTES
            GIBIBYTES -> TEBIBYTES
            TEBIBYTES -> PEBIBYTES
            else -> return this
        }
        return StorageSize(value / 1024, nextUnit).simplify()
    }

    override fun toString(): String = serialize()

    override fun compareTo(other: StorageSize): Int = inBytes.compareTo(other.inBytes)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as StorageSize

        if (inBytes != other.inBytes) return false

        return true
    }

    override fun hashCode(): Int {
        return inBytes.hashCode()
    }
}
