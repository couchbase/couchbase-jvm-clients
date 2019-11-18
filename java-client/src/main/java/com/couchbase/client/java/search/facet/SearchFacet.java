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


import com.couchbase.client.java.json.JsonObject;

import java.util.List;

/**
 * Base class for all FTS facets in querying.
 *
 * @author Simon Baslé
 * @author Michael Nitschinger
 * @since 2.3.0
 */
public abstract class SearchFacet {

    private final String field;
    private final int size;

    SearchFacet(String field, int size) {
        this.field = field;
        this.size = size;
    }

    public static TermFacet term(String field, int size) {
        return new TermFacet(field, size);
    }

    public static NumericRangeFacet numericRange(String field, int size, List<NumericRange> ranges) {
        return new NumericRangeFacet(field, size, ranges);
    }

    public static DateRangeFacet dateRange(String field, int size, List<DateRange> ranges) {
        return new DateRangeFacet(field, size, ranges);
    }

    public void injectParams(final JsonObject queryJson) {
        queryJson.put("size", size);
        queryJson.put("field", field);
    }

}
