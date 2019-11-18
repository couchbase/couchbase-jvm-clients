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

import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.search.SearchQuery;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * A FTS query that matches documents on a range of dates. At least one bound is required, and the parser
 * to use for the date (in {@link String} form) can be customized (see {@link #dateTimeParser(String)}).
 *
 * @author Simon Baslé
 * @author Michael Nitschinger
 * @since 2.3.0
 */
public class DateRangeQuery extends SearchQuery {

    private String start;
    private String  end;
    private Boolean inclusiveStart = null;
    private Boolean inclusiveEnd = null;
    private String dateTimeParser;
    private String field;

    public DateRangeQuery() {
        super();
    }

    /**
     * Sets the lower boundary of the range, inclusive or not depending on the second parameter.
     */
    public DateRangeQuery start(String start, boolean inclusive) {
        this.start = start;
        this.inclusiveStart = inclusive;
        return this;
    }

    /**
     * Sets the lower boundary of the range.
     * The lower boundary is considered inclusive by default on the server side.
     * @see #start(String, boolean)
     */
    public DateRangeQuery start(String start) {
        this.start = start;
        this.inclusiveStart = null;
        return this;
    }

    /**
     * Sets the upper boundary of the range, inclusive or not depending on the second parameter.
     */
    public DateRangeQuery end(String end, boolean inclusive) {
        this.end = end;
        this.inclusiveEnd = inclusive;
        return this;
    }

    /**
     * Sets the upper boundary of the range.
     * The upper boundary is considered exclusive by default on the server side.
     * @see #end(String, boolean)
     */
    public DateRangeQuery end(String end) {
        this.end = end;
        this.inclusiveEnd = null;
        return this;
    }


    /**
     * Sets the lower boundary of the range, inclusive or not depending on the second parameter.
     */
    public DateRangeQuery start(ZonedDateTime start, boolean inclusive) {
        this.start = start.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        this.inclusiveStart = inclusive;
        return this;
    }

    /**
     * Sets the lower boundary of the range.
     * The lower boundary is considered inclusive by default on the server side.
     */
    public DateRangeQuery start(ZonedDateTime start) {
        this.start = start.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        this.inclusiveStart = null;
        return this;
    }

    /**
     * Sets the upper boundary of the range, inclusive or not depending on the second parameter.
     */
    public DateRangeQuery end(ZonedDateTime end, boolean inclusive) {
        this.end = end.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        this.inclusiveEnd = inclusive;
        return this;
    }

    /**
     * Sets the upper boundary of the range.
     * The upper boundary is considered exclusive by default on the server side.
     */
    public DateRangeQuery end(ZonedDateTime end) {
        this.end = end.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        this.inclusiveEnd = null;
        return this;
    }

    /**
     * The name of the date/time parser to use to interpret {@link #start(String)} and {@link #end(String)}. Should not
     * be modified when passing in {@link ZonedDateTime}.
     */
    public DateRangeQuery dateTimeParser(final String dateTimeParser) {
        this.dateTimeParser = dateTimeParser;
        return this;
    }

    public DateRangeQuery field(String field) {
        this.field = field;
        return this;
    }

    @Override
    public DateRangeQuery boost(double boost) {
        super.boost(boost);
        return this;
    }

    @Override
    protected void injectParams(JsonObject input) {
        if (start == null && end == null) {
            throw new NullPointerException("DateRangeQuery needs at least one of start or end");
        }

        if (start != null) {
            input.put("start", start);
            if (inclusiveStart != null) {
                input.put("inclusive_start", inclusiveStart);
            }
        }
        if (end != null) {
            input.put("end", end);
            if (inclusiveEnd != null) {
                input.put("inclusive_end", inclusiveEnd);
            }
        }
        if (dateTimeParser != null) {
            input.put("datetime_parser", dateTimeParser);
        }
        if (field != null) {
            input.put("field", field);
        }
    }
}
