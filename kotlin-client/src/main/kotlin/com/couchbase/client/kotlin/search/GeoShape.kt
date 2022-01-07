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

package com.couchbase.client.kotlin.search

import com.couchbase.client.core.annotation.SinceCouchbase
import com.couchbase.client.kotlin.annotations.VolatileCouchbaseApi

public sealed class GeoShape {
    internal abstract fun inject(params: MutableMap<String, Any?>)

    public companion object {
        public fun circle(center: GeoPoint, radius: GeoDistance): GeoCircle = GeoCircle(center, radius)
        public fun rectangle(topLeft: GeoPoint, bottomRight: GeoPoint): GeoRectangle =
            GeoRectangle(topLeft, bottomRight)

        @SinceCouchbase("6.5.1")
        public fun polygon(vertices: List<GeoPoint>): GeoPolygon = GeoPolygon(vertices)
    }
}

public class GeoCircle(
    public val center: GeoPoint,
    public val radius: GeoDistance,
) : GeoShape() {
    override fun inject(params: MutableMap<String, Any?>) {
        params["location"] = center.serialize()
        params["distance"] = radius.serialize()
    }

    public companion object {
        @VolatileCouchbaseApi
        public infix fun GeoDistance.from(point: GeoPoint): GeoCircle = GeoCircle(point, this)
    }

    override fun toString(): String = "GeoCircle(center=$center, radius=$radius)"
}

public class GeoRectangle(
    public val topLeft: GeoPoint,
    public val bottomRight: GeoPoint,
) : GeoShape() {
    override fun inject(params: MutableMap<String, Any?>) {
        params["top_left"] = topLeft.serialize()
        params["bottom_right"] = bottomRight.serialize()
    }

    override fun toString(): String = "GeoRectangle(topLeft=$topLeft, bottomRight=$bottomRight)"
}

@SinceCouchbase("6.5.1")
public class GeoPolygon(
    public val vertices: List<GeoPoint>,
) : GeoShape() {
    init {
        require(vertices.size >= 3) { "GeoPolygon must have at least 3 vertices." }
    }

    override fun inject(params: MutableMap<String, Any?>) {
        params["polygon_points"] = vertices.map { it.serialize() }
    }

    override fun toString(): String = "GeoPolygon(vertices=$vertices)"
}
