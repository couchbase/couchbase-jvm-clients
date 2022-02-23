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

package com.couchbase.client.kotlin.manager.bucket

import com.couchbase.client.core.annotation.SinceCouchbase
import com.couchbase.client.core.json.Mapper
import com.couchbase.client.core.msg.kv.DurabilityLevel
import com.couchbase.client.kotlin.annotations.VolatileCouchbaseApi
import com.couchbase.client.kotlin.internal.putIfNotNull
import com.couchbase.client.kotlin.kv.Durability
import com.couchbase.client.kotlin.kv.internal.levelIfSynchronous
import com.couchbase.client.kotlin.manager.bucket.BucketType.Companion.MEMCACHED
import com.couchbase.client.kotlin.util.StorageSize
import com.couchbase.client.kotlin.util.StorageSize.Companion.bytes
import com.couchbase.client.kotlin.util.StorageSize.Companion.mebibytes
import kotlin.DeprecationLevel.WARNING
import kotlin.time.Duration.Companion.seconds

@JvmInline
public value class CompressionMode private constructor(
    internal val name: String,
) {
    public companion object {
        /**
         * Compressed documents are accepted but actively decompressed for storage in memory
         * and for streaming. Not advised!
         */
        public val OFF: CompressionMode = CompressionMode("off")

        /**
         * Compressed documents can be stored and streamed from the server, but the server does
         * not try to actively compress documents (client-initiated).
         */
        public val PASSIVE: CompressionMode = CompressionMode("passive")

        /**
         * The server will try to actively compress documents in memory.
         */
        public val ACTIVE: CompressionMode = CompressionMode("active")

        public fun of(name: String): CompressionMode = when (name) {
            OFF.name -> OFF
            PASSIVE.name -> PASSIVE
            ACTIVE.name -> ACTIVE
            else -> CompressionMode(name)
        }
    }

    override fun toString(): String = name
}

@JvmInline
public value class BucketType private constructor(
    internal val name: String,
) {
    public companion object {
        /**
         * Stores data persistently, as well as in memory. It allows data to be automatically replicated for high
         * availability, using the Database Change Protocol (DCP); and dynamically scaled across multiple clusters,
         * by means of Cross Datacenter Replication (XDCR).
         */
        public val COUCHBASE: BucketType = BucketType("membase")

        /**
         * Ephemeral buckets are an alternative to Couchbase buckets, to be used whenever persistence is not required: for
         * example, when repeated disk-access involves too much overhead. This allows highly consistent in-memory performance,
         * without disk-based fluctuations. It also allows faster node rebalances and restarts.
         */
        public val EPHEMERAL: BucketType = BucketType("ephemeral")

        /**
         * Memcached buckets are designed to be used alongside other database platforms,
         * such as ones employing relational database technology. By caching frequently-used data, Memcached
         * buckets reduce the number of queries a database-server must perform. Each Memcached bucket provides a
         * directly addressable, distributed, in-memory key-value cache.
         */
        @Deprecated(level = WARNING,
            message = "Memcached buckets are deprecated. Consider using an ephemeral bucket instead.")
        public val MEMCACHED: BucketType = BucketType("memcached")

        public fun of(name: String): BucketType = when (name) {
            COUCHBASE.name -> COUCHBASE
            EPHEMERAL.name -> EPHEMERAL
            MEMCACHED.name -> MEMCACHED
            else -> BucketType(name)
        }
    }

    override fun toString(): String = name
}

/**
 * A conflict is caused when the source and target copies of an XDCR-replicated document are updated independently
 * of and dissimilarly to one another, each by a local application. The conflict must be resolved, by determining
 * which of the variants should prevail; and then correspondingly saving both documents in identical form. XDCR
 * provides an automated conflict resolution process.
 */
@JvmInline
public value class ConflictResolutionType private constructor(
    internal val name: String,
) {
    public companion object {
        /**
         * Timestamp-based conflict resolution (often referred to as Last Write Wins, or LWW) uses the document
         * timestamp (stored in the CAS) to resolve conflicts. The timestamps associated with the most recent
         * updates of source and target documents are compared. The document whose update has the more recent
         * timestamp prevails.
         */
        public val TIMESTAMP: ConflictResolutionType = ConflictResolutionType("lww")

        /**
         * Conflicts can be resolved by referring to documents' sequence numbers. Sequence numbers are maintained
         * per document, and are incremented on every document-update. The sequence numbers of source and
         * target documents are compared; and the document with the higher sequence number prevails.
         */
        public val SEQUENCE_NUMBER: ConflictResolutionType = ConflictResolutionType("seqno")

        /**
         * In Couchbase Server 7.1, this feature is only available in "developer-preview" mode.
         * See the UI XDCR settings for the custom conflict resolution properties.
         */
        @VolatileCouchbaseApi
        @SinceCouchbase("7.1") // developer preview
        public val CUSTOM: ConflictResolutionType = ConflictResolutionType("custom")

        public fun of(name: String): ConflictResolutionType = when (name) {
            TIMESTAMP.name -> TIMESTAMP
            SEQUENCE_NUMBER.name -> SEQUENCE_NUMBER
            CUSTOM.name -> CUSTOM
            else -> ConflictResolutionType(name)
        }
    }

    override fun toString(): String = name
}

@JvmInline
public value class EvictionPolicyType private constructor(
    internal val name: String,
) {
    public companion object {
        /**
         * During ejection, everything (including key, metadata, and value) will be ejected.
         *
         * Full Ejection reduces the memory overhead requirement, at the cost of performance.
         *
         * This value is only valid for buckets of type [BucketType.COUCHBASE].
         */
        public val FULL: EvictionPolicyType = EvictionPolicyType("fullEviction")

        /**
         * During ejection, only the value will be ejected (key and metadata will remain in memory).
         *
         * Value Ejection needs more system memory, but provides better performance than Full Ejection.
         *
         * This value is only valid for buckets of type [BucketType.COUCHBASE].
         */
        public val VALUE_ONLY: EvictionPolicyType = EvictionPolicyType("valueOnly")

        /**
         * When the memory quota is reached, Couchbase Server ejects data that has
         * not been used recently.
         *
         * This value is only valid for buckets of type [BucketType.EPHEMERAL].
         */
        public val NOT_RECENTLY_USED: EvictionPolicyType = EvictionPolicyType("nruEviction")

        /**
         * Couchbase Server keeps all data until explicitly deleted, but will reject
         * any new data if you reach the quota (dedicated memory) you set for your bucket.
         *
         * This value is only valid for buckets of type [BucketType.EPHEMERAL].
         */
        public val NO_EVICTION: EvictionPolicyType = EvictionPolicyType("noEviction")

        public fun of(name: String): EvictionPolicyType = when (name) {
            FULL.name -> FULL
            VALUE_ONLY.name -> VALUE_ONLY
            NOT_RECENTLY_USED.name -> NOT_RECENTLY_USED
            NO_EVICTION.name -> NO_EVICTION
            else -> EvictionPolicyType(name)
        }
    }

    override fun toString(): String = name
}

@JvmInline
public value class StorageBackend private constructor(
    internal val name: String,
) {
    public companion object {
        public val COUCHSTORE: StorageBackend = StorageBackend("couchstore")

        @SinceCouchbase("7.1")
        public val MAGMA: StorageBackend = StorageBackend("magma")

        public fun of(name: String): StorageBackend = when (name) {
            COUCHSTORE.name -> COUCHSTORE
            MAGMA.name -> MAGMA
            else -> StorageBackend(name)
        }
    }

    override fun toString(): String = name
}

public class BucketSettings(
    public val name: String,
    public val ramQuota: StorageSize = 100.mebibytes,
    public val bucketType: BucketType = BucketType.COUCHBASE,
    public val storageBackend: StorageBackend? = null, // null means default for the bucket type
    public val flushEnabled: Boolean = false,
    public val replicas: Int = 1,
    public val maximumExpiry: kotlin.time.Duration? = null,
    public val compressionMode: CompressionMode = CompressionMode.PASSIVE,
    public val conflictResolutionType: ConflictResolutionType = ConflictResolutionType.SEQUENCE_NUMBER,
    public val minimumDurability: Durability = Durability.none(),
    public val evictionPolicy: EvictionPolicyType? = null, // null means default for the bucket type
) {

    init {
        require(minimumDurability !is Durability.ClientVerified) {
            "Minimum durability must not be client verified."
        }
    }

    public fun copy(
        name: String = this.name,
        ramQuota: StorageSize = this.ramQuota,
        bucketType: BucketType = this.bucketType,
        storageBackend: StorageBackend? = this.storageBackend,
        flushEnabled: Boolean = this.flushEnabled,
        replicas: Int = this.replicas,
        maximumExpiry: kotlin.time.Duration? = this.maximumExpiry,
        compressionMode: CompressionMode = this.compressionMode,
        conflictResolutionType: ConflictResolutionType = this.conflictResolutionType,
        minimumDurability: Durability = this.minimumDurability,
        evictionPolicy: EvictionPolicyType? = this.evictionPolicy,
    ): BucketSettings = BucketSettings(
        name = name,
        ramQuota = ramQuota,
        bucketType = bucketType,
        storageBackend = storageBackend,
        flushEnabled = flushEnabled,
        replicas = replicas,
        maximumExpiry = maximumExpiry,
        compressionMode = compressionMode,
        conflictResolutionType = conflictResolutionType,
        minimumDurability = minimumDurability,
        evictionPolicy = evictionPolicy,
    )

    public companion object {

        internal fun fromJson(jsonBytes: ByteArray): BucketSettings {
            val json = Mapper.decodeIntoTree(jsonBytes)

            return BucketSettings(
                name = json.get("name").textValue(),
                flushEnabled = !json.path("controllers").path("flush").isMissingNode,
                ramQuota = json.path("quota").path("rawRAM").longValue().bytes.simplify(),
                replicas = json.path("replicaNumber").intValue(),
                maximumExpiry = json.path("maxTTL").asLong().let { if (it == 0L) null else it.seconds },

                // Couchbase 5.0 doesn't send a compressionMode
                compressionMode = CompressionMode.of(json.path("compressionMode").asText(CompressionMode.OFF.name)),

                bucketType = BucketType.of(json.path("bucketType").asText()),
                conflictResolutionType = ConflictResolutionType.of(json.path("conflictResolutionType").asText()),
                minimumDurability = Durability.of(
                    DurabilityLevel.decodeFromManagementApi(json.path("durabilityMinLevel").textValue())
                ),
                evictionPolicy = EvictionPolicyType.of(json.path("evictionPolicy").asText()),
                storageBackend = StorageBackend.of(json.path("storageBackend").asText())
            )
        }
    }

    internal fun toMap(): Map<String, String> {
        val params = mutableMapOf<String, Any?>()
        params["ramQuotaMB"] = ramQuota.inWholeMebibytes

        params["flushEnabled"] = if (flushEnabled) 1 else 0

        // Do not send if it's been left at default, else will get an error on CE
        params.putIfNotNull("maxTTL", maximumExpiry?.inWholeSeconds)

        // If null, let server assign the default policy for this bucket type
        params.putIfNotNull("evictionPolicy", evictionPolicy?.name)

        // Do not send if it's been left at default, else will get an error on CE
        if (compressionMode != CompressionMode.PASSIVE) params["compressionMode"] = compressionMode.name

        if (minimumDurability != Durability.None) {
            params.putIfNotNull("durabilityMinLevel",
                minimumDurability.levelIfSynchronous().map { it.encodeForManagementApi() }.orElse(null))
        }

        params.putIfNotNull("storageBackend", storageBackend?.name)

        params["name"] = name
        params["bucketType"] = bucketType.name
        params["conflictResolutionType"] = conflictResolutionType.name

        if (bucketType != MEMCACHED) params["replicaNumber"] = replicas

        return params.mapValues { (_, v) -> v.toString() }
    }

    override fun toString(): String {
        return "BucketSettings(name='$name', ramQuota=$ramQuota, bucketType=$bucketType, storageBackend=$storageBackend, flushEnabled=$flushEnabled, replicas=$replicas, maximumExpiry=$maximumExpiry, compressionMode=$compressionMode, conflictResolutionType=$conflictResolutionType, minimumDurability=$minimumDurability, evictionPolicy=$evictionPolicy)"
    }
}
