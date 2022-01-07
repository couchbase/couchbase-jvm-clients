package com.couchbase.client.kotlin.search

public enum class GeoDistanceUnit(
    internal val value: String,
) {
    MILLIMETERS("mm"),
    CENTIMETERS("cm"),
    METERS("m"),
    KILOMETERS("km"),

    INCHES("in"),
    FEET("ft"),
    YARDS("yd"),
    MILES("mi"),

    NAUTICAL_MILES("nm"),
}

/**
 * Create instances using the Int extensions. Example:
 * ```
 * val walkingDistance = 400.meters
 * val marathon = 42_195.meters
 * val averageElectricCarRange = 181.miles
 * val berlinToLondon = 1_100.kilometers
 * ```
 */
public class GeoDistance(
    public val value: Int,
    public val unit: GeoDistanceUnit,
) {
    public companion object {
        public inline val Int.millimeters: GeoDistance get() = GeoDistance(this, GeoDistanceUnit.MILLIMETERS)
        public inline val Int.centimeters: GeoDistance get() = GeoDistance(this, GeoDistanceUnit.CENTIMETERS)
        public inline val Int.meters: GeoDistance get() = GeoDistance(this, GeoDistanceUnit.METERS)
        public inline val Int.kilometers: GeoDistance get() = GeoDistance(this, GeoDistanceUnit.KILOMETERS)
        public inline val Int.inches: GeoDistance get() = GeoDistance(this, GeoDistanceUnit.INCHES)
        public inline val Int.feet: GeoDistance get() = GeoDistance(this, GeoDistanceUnit.FEET)
        public inline val Int.yards: GeoDistance get() = GeoDistance(this, GeoDistanceUnit.YARDS)
        public inline val Int.miles: GeoDistance get() = GeoDistance(this, GeoDistanceUnit.MILES)
        public inline val Int.nauticalMiles: GeoDistance get() = GeoDistance(this, GeoDistanceUnit.NAUTICAL_MILES)
    }

    internal fun serialize(): String {
        return "$value${unit.value}"
    }

    override fun toString(): String {
        return serialize()
    }
}
