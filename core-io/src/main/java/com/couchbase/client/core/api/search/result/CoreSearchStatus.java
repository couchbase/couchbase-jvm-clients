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

import java.util.Map;
import java.util.Objects;

@Stability.Internal
@JsonIgnoreProperties(ignoreUnknown = true)
public class CoreSearchStatus {

    private final long errorCount;
    private final long successCount;
    private final Map<String, String> errors;

    @JsonCreator
    private CoreSearchStatus(
      @JsonProperty("failed") final long errorCount,
      @JsonProperty("successful") final long successCount,
      @JsonProperty("errors") final Map<String, String> errors) {
        this.errorCount = errorCount;
        this.successCount = successCount;
        this.errors = errors;
    }

    public long successCount() {
        return this.successCount;
    }

    public long errorCount() {
        return this.errorCount;
    }

    public Map<String, String> errors() {
        return errors;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CoreSearchStatus that = (CoreSearchStatus) o;
        return errorCount == that.errorCount &&
          successCount == that.successCount &&
          Objects.equals(errors, that.errors);
    }

    @Override
    public int hashCode() {
        return Objects.hash(errorCount, successCount, errors);
    }

    @Override
    public String toString() {
        return "SearchStatus{" +
          "errorCount=" + errorCount +
          ", successCount=" + successCount +
          ", errors=" + errors +
          '}';
    }

}
