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
package com.couchbase.client.java.search.result;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.api.search.result.CoreSearchDateRange;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;
import java.util.Objects;

/**
 * A range (or bucket) for a {@link DateRangeSearchFacetResult}. Counts the number of matches
 * that fall into the named range (which can overlap with other user-defined ranges).
 *
 * @author Simon Baslé
 * @author Michael Nitschinger
 * @since 2.3.0
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SearchDateRange {

    private final CoreSearchDateRange internal;

    @Stability.Internal
    public SearchDateRange(CoreSearchDateRange internal) {
        this.internal = internal;
    }

    public String name() {
        return internal.name();
    }

    public Instant start() {
        return internal.start();
    }

    public Instant end() {
        return internal.end();
    }

    public long count() {
        return internal.count();
    }

    @Override
    public String toString() {
        return internal.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchDateRange searchRow = (SearchDateRange) o;
        return Objects.equals(internal, searchRow.internal);
    }

    @Override
    public int hashCode() {
        return internal.hashCode();
    }
}
