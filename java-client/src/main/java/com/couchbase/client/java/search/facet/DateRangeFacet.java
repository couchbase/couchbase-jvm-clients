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
 * A facet that categorizes rows inside date ranges (or buckets) provided by the user.
 *
 * @author Simon Baslé
 * @author Michael Nitschinger
 * @since 2.3.0
 */
public class DateRangeFacet extends SearchFacet {

    private final List<DateRange> dateRanges;

    DateRangeFacet(String field, int limit, List<DateRange> dateRanges) {
        super(field, limit);
        this.dateRanges = dateRanges;
    }

    @Override
    public void injectParams(JsonObject queryJson) {
        super.injectParams(queryJson);

        JsonArray dateRange = JsonArray.create();
        for (DateRange dr : dateRanges) {
            JsonObject drJson = JsonObject.create();
            drJson.put("name", dr.name());

            if (dr.start() != null) {
                drJson.put("start", dr.start());
            }
            if (dr.end() != null) {
                drJson.put("end", dr.end());
            }

            dateRange.add(drJson);
        }
        queryJson.put("date_ranges", dateRange);
    }

}
