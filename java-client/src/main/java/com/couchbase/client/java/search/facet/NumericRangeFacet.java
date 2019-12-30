/*
 * Copyright (c) 2016 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.client.java.search.facet;

import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;

import java.util.List;

/**
 * A facet that categorizes rows into numerical ranges (or buckets) provided by the user.
 *
 * @author Simon Baslé
 * @author Michael Nitschinger
 * @since 2.3.0
 */
public class NumericRangeFacet extends SearchFacet {

    private final List<NumericRange> ranges;

    NumericRangeFacet(String field, int limit, List<NumericRange> ranges) {
        super(field, limit);
        this.ranges = ranges;
    }

    @Override
    public void injectParams(JsonObject queryJson) {
        super.injectParams(queryJson);

        JsonArray numericRange = JsonArray.create();
        for (NumericRange nr : ranges) {
            JsonObject nrJson = JsonObject.create();
            nrJson.put("name", nr.name());

            if (nr.min() != null) {
                nrJson.put("min", nr.min());
            }
            if (nr.max() != null) {
                nrJson.put("max", nr.max());
            }

            numericRange.add(nrJson);
        }
        queryJson.put("numeric_ranges", numericRange);
    }
}
