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

import java.util.Objects;

@Stability.Internal
@JsonIgnoreProperties(ignoreUnknown = true)
public class CoreSearchNumericRange {

    private final String name;
    private final double min;
    private final double max;
    private final long count;

    @JsonCreator
    public CoreSearchNumericRange(
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
        CoreSearchNumericRange that = (CoreSearchNumericRange) o;
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
