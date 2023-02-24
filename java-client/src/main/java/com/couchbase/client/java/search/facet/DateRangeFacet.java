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

import com.couchbase.client.core.api.search.facet.CoreDateRange;
import com.couchbase.client.core.api.search.facet.CoreDateRangeFacet;
import com.couchbase.client.core.api.search.facet.CoreSearchFacet;

import java.util.List;
import java.util.stream.Collectors;

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
    public CoreSearchFacet toCore() {
        return new CoreDateRangeFacet(field, size, dateRanges.stream()
                .map(v -> new CoreDateRange(v.name(), v.start, v.end))
                .collect(Collectors.toList()));
    }
}
