/*
 * Copyright (c) 2023 Couchbase, Inc.
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
package com.couchbase.client.core.api.search.queries;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.search.CoreSearchQuery;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.node.ObjectNode;
import org.jspecify.annotations.Nullable;

@Stability.Internal
public class CoreDateRangeQuery extends CoreSearchQuery {

    public final @Nullable String start;
    public final @Nullable String end;
    public final @Nullable Boolean inclusiveStart;
    public final @Nullable Boolean inclusiveEnd;
    public final @Nullable String dateTimeParser;
    public final @Nullable String field;

    public CoreDateRangeQuery(@Nullable String start,
                              @Nullable String end,
                              @Nullable Boolean inclusiveStart,
                              @Nullable Boolean inclusiveEnd,
                              @Nullable String dateTimeParser,
                              @Nullable String field,
                              @Nullable Double boost) {
        super(boost);

        if (start == null && end == null) {
            throw new NullPointerException("DateRangeQuery needs at least one of start or end");
        }

        this.start = start;
        this.end = end;
        this.inclusiveStart = inclusiveStart;
        this.inclusiveEnd = inclusiveEnd;
        this.dateTimeParser = dateTimeParser;
        this.field = field;
    }

    @Override
    protected void injectParams(ObjectNode input) {
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

    @Override
    public <T> T convert(CoreSearchQueryConverter<T> converter) {
        return converter.convert(this);
    }
}
