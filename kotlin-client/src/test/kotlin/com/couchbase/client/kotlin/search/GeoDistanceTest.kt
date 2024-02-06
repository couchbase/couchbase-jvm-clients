/*
 * Copyright 2024 Couchbase, Inc.
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

package com.couchbase.client.kotlin.search

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

internal class GeoDistanceTest {
    @Test
    fun `can parse all units`() {
        val nameToUnit = mapOf(
            "mi" to GeoDistanceUnit.MILES,
            "miles" to GeoDistanceUnit.MILES,
            "mm" to GeoDistanceUnit.MILLIMETERS,
            "millimeters" to GeoDistanceUnit.MILLIMETERS,
            "cm" to GeoDistanceUnit.CENTIMETERS,
            "centimeters" to GeoDistanceUnit.CENTIMETERS,
            "m" to GeoDistanceUnit.METERS,
            "meters" to GeoDistanceUnit.METERS,
            "km" to GeoDistanceUnit.KILOMETERS,
            "kilometers" to GeoDistanceUnit.KILOMETERS,
            "in" to GeoDistanceUnit.INCHES,
            "inch" to GeoDistanceUnit.INCHES, // irregular (singular) :-(
            "ft" to GeoDistanceUnit.FEET,
            "feet" to GeoDistanceUnit.FEET,
            "yd" to GeoDistanceUnit.YARDS,
            "yards" to GeoDistanceUnit.YARDS,
            "mi" to GeoDistanceUnit.MILES,
            "miles" to GeoDistanceUnit.MILES,
            "nm" to GeoDistanceUnit.NAUTICAL_MILES,
            "nauticalmiles" to GeoDistanceUnit.NAUTICAL_MILES,
        )

        nameToUnit.forEach { (name, unit) ->
            for (value in listOf(0, 1, 10, 100)) {
                val parsed = GeoDistance.parse("$value$name")
                assertEquals(unit, parsed.unit)
                assertEquals(value, parsed.value)
            }
        }
    }

    @Test
    fun `parser does not accept whitespace or nonsense`() {
        for (invalid in listOf(
            "m", // missing value
            "3", // missing unit
            " 3m", "3m ", "3 m", // extra whitespace
            "-3m", // negative value
            "4z", // invalid unit
            "${Long.MAX_VALUE}m", // value outside range of Int
            "abcde", // garbage
        )) {
            assertThrows<IllegalArgumentException> { GeoDistance.parse(invalid) }
        }
    }
}
