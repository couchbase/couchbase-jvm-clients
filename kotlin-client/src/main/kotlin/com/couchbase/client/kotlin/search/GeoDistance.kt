package com.couchbase.client.kotlin.search

import com.couchbase.client.core.api.search.sort.CoreSearchGeoDistanceUnits
import com.couchbase.client.kotlin.annotations.VolatileCouchbaseApi

public enum class GeoDistanceUnit(
    internal val value: String,
    internal val core: CoreSearchGeoDistanceUnits
) {
    MILLIMETERS("mm", CoreSearchGeoDistanceUnits.MILLIMETERS),
    CENTIMETERS("cm", CoreSearchGeoDistanceUnits.CENTIMETERS),
    METERS("m", CoreSearchGeoDistanceUnits.METERS),
    KILOMETERS("km", CoreSearchGeoDistanceUnits.KILOMETERS),

    INCHES("in", CoreSearchGeoDistanceUnits.INCH),
    FEET("ft", CoreSearchGeoDistanceUnits.FEET),
    YARDS("yd", CoreSearchGeoDistanceUnits.YARDS),
    MILES("mi", CoreSearchGeoDistanceUnits.MILES),

    NAUTICAL_MILES("nm", CoreSearchGeoDistanceUnits.NAUTICAL_MILES),
    ;
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

        @VolatileCouchbaseApi
        public fun parse(formatted: String): GeoDistance {
            val result = distanceRegex.matchEntire(formatted)
                ?: throw IllegalArgumentException(
                    "Malformed GeoDistance string." +
                            " Expected an integer followed immediately by a unit name (or abbreviation)," +
                            " but got: '$formatted')"
                )

            val value = result.groups["value"]!!.value.toInt()
            val unitName = result.groups["unit"]!!.value

            val unit = unitNameMap[unitName]
                ?: throw IllegalArgumentException(
                    "Malformed GeoDistance string. Unit must be one of ${unitNameMap.keys}, but got: '$unitName'"
                )
            return GeoDistance(value, unit)
        }
    }

    internal fun serialize(): String {
        return "$value${unit.value}"
    }

    override fun toString(): String {
        return serialize()
    }
}

private val distanceRegex = Regex("""(?<value>\d+)(?<unit>\w+)""")

private val unitNameMap = GeoDistanceUnit.values().flatMap {
    listOf(
        it.value to it, // abbreviated name
        it.core.identifier() to it, // full name
    )
}.toMap()
