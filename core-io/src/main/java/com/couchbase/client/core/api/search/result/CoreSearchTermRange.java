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

@Stability.Internal
@JsonIgnoreProperties(ignoreUnknown = true)
public class CoreSearchTermRange {

    private final String name;
    private final long count;

    @JsonCreator
    public CoreSearchTermRange(
      @JsonProperty("term") String name,
      @JsonProperty("count") long count) {
        this.name = name;
        this.count = count;
    }

    public String name() {
        return name;
    }

    public long count() {
        return count;
    }

    @Override
    public String toString() {
        return "{" + "name='" + name + '\'' +
          ", count=" + count +
          '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CoreSearchTermRange termRange = (CoreSearchTermRange) o;

        if (count != termRange.count) {
            return false;
        }
        return name.equals(termRange.name);

    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + (int) (count ^ (count >>> 32));
        return result;
    }
}
