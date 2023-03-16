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
package com.couchbase.client.java.search.queries;

import com.couchbase.client.core.api.search.CoreSearchQuery;
import com.couchbase.client.core.api.search.queries.CoreNumericRangeQuery;
import com.couchbase.client.java.search.SearchQuery;

/**
 * A FTS query that matches documents on a range of values. At least one bound is required, and the
 * inclusiveness of each bound can be configured.
 *
 * @author Simon Baslé
 * @author Michael Nitschinger
 * @since 2.3.0
 */
public class NumericRangeQuery extends SearchQuery {

    private Double min;
    private Double max;
    private Boolean inclusiveMin = null;
    private Boolean inclusiveMax = null;
    private String field;

    public NumericRangeQuery() {
        super();
    }

    /**
     * Sets the lower boundary of the range, inclusive or not depending on the second parameter.
     */
    public NumericRangeQuery min(double min, boolean inclusive) {
        this.min = min;
        this.inclusiveMin = inclusive;
        return this;
    }

    /**
     * Sets the lower boundary of the range.
     * The lower boundary is considered inclusive by default on the server side.
     * @see #min(double, boolean)
     */
    public NumericRangeQuery min(double min) {
        this.min = min;
        this.inclusiveMin = null;
        return this;
    }

    /**
     * Sets the upper boundary of the range, inclusive or not depending on the second parameter.
     */
    public NumericRangeQuery max(double max, boolean inclusive) {
        this.max = max;
        this.inclusiveMax = inclusive;
        return this;
    }

    /**
     * Sets the upper boundary of the range.
     * The upper boundary is considered exclusive by default on the server side.
     * @see #max(double, boolean)
     */
    public NumericRangeQuery max(double max) {
        this.max = max;
        this.inclusiveMax = null;
        return this;
    }

    public NumericRangeQuery field(String field) {
        this.field = field;
        return this;
    }

    @Override
    public NumericRangeQuery boost(double boost) {
        super.boost(boost);
        return this;
    }

    @Override
    public CoreSearchQuery toCore() {
        return new CoreNumericRangeQuery(min, max, inclusiveMin, inclusiveMax, field, boost);
    }
}
