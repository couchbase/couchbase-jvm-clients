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

import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonCreator;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.couchbase.client.core.deps.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * A range (or bucket) for a {@link NumericRangeSearchFacetResult}. Counts the number of matches
 * that fall into the named range (which can overlap with other user-defined ranges in the facet).
 *
 * @author Simon Baslé
 * @author Michael Nitschinger
 * @since 2.3.0
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SearchNumericRange {

    private final String name;
    private final double min;
    private final double max;
    private final long count;

    @JsonCreator
    public SearchNumericRange(
      @JsonProperty("name") String name,
      @JsonProperty("min") double min,
      @JsonProperty("max") double max,
      @JsonProperty("count") long count) {
        this.name = name;
        this.min = min;
        this.max = max;
        this.count = count;
    }

    public String name() {
        return name;
    }

    public Double min() {
        return min;
    }

    public Double max() {
        return max;
    }

    public long count() {
        return count;
    }

    @Override
    public String toString() {
        return "SearchNumericRange{" +
          "name='" + name + '\'' +
          ", min=" + min +
          ", max=" + max +
          ", count=" + count +
          '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchNumericRange that = (SearchNumericRange) o;
        return Double.compare(that.min, min) == 0 &&
          Double.compare(that.max, max) == 0 &&
          count == that.count &&
          Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, min, max, count);
    }
}
