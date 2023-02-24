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
package com.couchbase.client.core.api.search.result;

import com.couchbase.client.core.annotation.Stability;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonCreator;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Objects;

/**
 * A range (or bucket) for a {@link CoreDateRangeSearchFacetResult}. Counts the number of matches
 * that fall into the named range (which can overlap with other user-defined ranges).
 *
 * @author Simon Baslé
 * @author Michael Nitschinger
 * @since 2.3.0
 */
@Stability.Internal
@JsonIgnoreProperties(ignoreUnknown = true)
public class CoreSearchDateRange {

    private final String name;
    private final Instant start;
    private final Instant end;
    private final long count;

    @JsonCreator
    public CoreSearchDateRange(
      @JsonProperty("name") String name,
      @JsonProperty("start") String start,
      @JsonProperty("end") String end,
      @JsonProperty("count") long count) {
        this.name = name;
        this.count = count;

        this.start = start == null ? null : Instant.parse(start);
        this.end = end == null ? null : Instant.parse(end);
    }

    public String name() {
        return name;
    }

    public Instant start() {
        return start;
    }

    public Instant end() {
        return end;
    }

    public long count() {
        return count;
    }

    @Override
    public String toString() {
        return "SearchDateRange{" +
          "name='" + name + '\'' +
          ", start=" + start +
          ", end=" + end +
          ", count=" + count +
          '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CoreSearchDateRange that = (CoreSearchDateRange) o;
        return count == that.count &&
          Objects.equals(name, that.name) &&
          Objects.equals(start, that.start) &&
          Objects.equals(end, that.end);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, start, end, count);
    }
}
