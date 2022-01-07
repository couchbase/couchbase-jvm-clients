package com.couchbase.client.kotlin.search

import com.couchbase.client.core.annotation.SinceCouchbase
import com.couchbase.client.kotlin.internal.MustUseNamedArguments
import com.couchbase.client.kotlin.search.GeoPoint.Companion.coordinates
import com.couchbase.client.kotlin.search.GeoPoint.Companion.geohash

/**
 * No matter where you go, there you are.
 *
 * Create an instance using [geohash] or [coordinates].
 */
public sealed class GeoPoint {
    internal abstract fun serialize(): Any
    override fun toString(): String = serialize().toString()

    public companion object {
        /**
         * Specifies a geographic point using a [geohash](https://en.wikipedia.org/wiki/Geohash).
         * ```
         * val eiffelTower = geohash("u09tunquc")
         * ```
         */
        @SinceCouchbase("6.5")
        public fun geohash(value: String): GeoHash = GeoHash(value)

        /**
         * Specifies a geographic point using latitude and longitude.
         *
         * Must be called with named arguments. Example:
         * ```
         * val eiffelTower = coordinates(lat = 48.8584, lon = 2.2945)
         * ```
         * @param lat latitude coordinate
         * @param lon longitude coordinate
         */
        public fun coordinates(
            @Suppress("UNUSED_PARAMETER") vararg ignored: MustUseNamedArguments,
            lon: Double,
            lat: Double,
        ): GeoCoordinates = GeoCoordinates(lon, lat)
    }
}

@SinceCouchbase("6.5")
public class GeoHash internal constructor(
    public val value: String,
) : GeoPoint() {
    override fun serialize(): Any = value
}

public class GeoCoordinates internal constructor(
    public val lon: Double,
    public val lat: Double,
) : GeoPoint() {
    override fun serialize(): Any = listOf(lon, lat)
}
