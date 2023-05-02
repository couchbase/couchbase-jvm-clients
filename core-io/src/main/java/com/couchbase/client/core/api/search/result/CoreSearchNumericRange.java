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
import reactor.util.annotation.Nullable;

import java.util.Objects;

@Stability.Internal
@JsonIgnoreProperties(ignoreUnknown = true)
public class CoreSearchNumericRange {

    private final String name;
    @Nullable private final Double min;
    @Nullable private final Double max;
    private final long count;

    @JsonCreator
    public CoreSearchNumericRange(
      @JsonProperty("name") String name,
      @Nullable @JsonProperty("min") Double min,
      @Nullable @JsonProperty("max") Double max,
      @JsonProperty("count") long count) {
        this.name = name;
        this.min = min;
        this.max = max;
        this.count = count;
    }

    public String name() {
        return name;
    }

    @Nullable
    public Double min() {
        return min;
    }

    @Nullable
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
        return count == that.count && name.equals(that.name) && Objects.equals(min, that.min) && Objects.equals(max, that.max);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, min, max, count);
    }
}
